#include "doctest/doctest.h"
#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <rest_rpc.hpp>
#include <string>
#include <thread>
#include <vector>

using namespace rest_rpc;
using namespace rpc_service;

// The client fails to read a response body: the server sends a full response
// header, part of the body, then shuts down its sending side. The client must
// close the connection and drop what it has queued, so that a later
// connection works.
namespace {

bool wait_until(const std::function<bool()> &done,
                std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!done()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return true;
}

int add(rpc_conn, int a, int b) { return a + b; }

// A peer that speaks the wire format by hand. All waits are bounded.
class fake_server {
public:
  // receive_buffer is set before listen() so that it limits the window.
  explicit fake_server(int receive_buffer = 0) : acceptor_(ios_), socket_(ios_) {
    asio::ip::tcp::endpoint endpoint(asio::ip::address::from_string("127.0.0.1"),
                                     0);
    acceptor_.open(endpoint.protocol());
    if (receive_buffer > 0) {
      acceptor_.set_option(asio::socket_base::receive_buffer_size(receive_buffer));
    }
    acceptor_.bind(endpoint);
    acceptor_.listen();
  }

  unsigned short port() const { return acceptor_.local_endpoint().port(); }

  bool accept() {
    bool done = false;
    acceptor_.async_accept(socket_, [&done](const asio::error_code &ec) {
      done = !ec;
    });
    run_for(std::chrono::seconds(5));
    return done;
  }

  // Reads one request, header and body.
  bool read_request(rpc_header &header) {
    if (!read_exactly(&header, sizeof(header))) {
      return false;
    }
    std::vector<char> body(header.body_len);
    return body.empty() || read_exactly(body.data(), body.size());
  }

  // Answers the request with a header announcing body_len bytes, sends only
  // sent bytes of the body and shuts the sending side down.
  void reply_partially(const rpc_header &request, uint32_t body_len,
                       size_t sent) {
    rpc_header header{MAGIC_NUM, request_type::req_res, body_len,
                      request.req_id, request.func_id};
    std::vector<char> body(sent, 'x');
    asio::write(socket_, asio::buffer(&header, sizeof(header)));
    asio::write(socket_, asio::buffer(body));
    socket_.shutdown(asio::ip::tcp::socket::shutdown_send);
  }

  // Whether the client closes its side within the timeout. Discards what the
  // client sends meanwhile.
  bool wait_closed_by_client(std::chrono::milliseconds timeout) {
    bool closed = false;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!closed && std::chrono::steady_clock::now() < deadline) {
      bool done = false;
      socket_.async_read_some(asio::buffer(scratch_),
                              [&](const asio::error_code &ec, size_t) {
                                done = true;
                                // eof or reset; operation_aborted is the
                                // cancel below
                                closed = ec && ec != asio::error::operation_aborted;
                              });
      run_for(deadline - std::chrono::steady_clock::now());
      if (!done) {
        socket_.cancel();
        run_for(std::chrono::seconds(1));
      }
    }
    return closed;
  }

  size_t available() { return socket_.available(); }

private:
  bool read_exactly(void *data, size_t size) {
    bool done = false;
    asio::async_read(socket_, asio::buffer(data, size),
                     [&done](const asio::error_code &ec, size_t) { done = !ec; });
    run_for(std::chrono::seconds(5));
    return done;
  }

  template <typename Duration> void run_for(Duration timeout) {
    ios_.restart();
    ios_.run_for(timeout);
  }

  asio::io_context ios_;
  asio::ip::tcp::acceptor acceptor_;
  asio::ip::tcp::socket socket_;
  std::array<char, 65536> scratch_;
};

struct call_result {
  std::atomic<bool> done{false};
  std::atomic<bool> failed{false};
  std::atomic<bool> connected_in_callback{true};
};

// Calls "add" through the callback interface and records the outcome.
std::shared_ptr<call_result> async_add(rpc_client &client) {
  auto result = std::make_shared<call_result>();
  client.async_call<int>(
      "add",
      [result, &client](asio::error_code ec, int) {
        result->connected_in_callback = client.has_connected();
        result->failed = static_cast<bool>(ec);
        result->done = true;
      },
      1, 2);
  return result;
}

} // namespace

TEST_SUITE("client_error_close") {
  TEST_CASE("body_read_error_closes_the_connection") {
    fake_server server;
    rpc_client client;
    client.async_connect("127.0.0.1", server.port());
    REQUIRE(server.accept());
    REQUIRE(wait_until([&client] { return client.has_connected(); },
                       std::chrono::seconds(5)));

    auto call = async_add(client);
    rpc_header request{};
    REQUIRE(server.read_request(request));
    server.reply_partially(request, 100, 10);

    CHECK(wait_until([&call] { return call->done.load(); },
                     std::chrono::seconds(5)));
    CHECK(call->failed);
    // The request callback runs after the client is marked disconnected.
    CHECK_FALSE(call->connected_in_callback);
    CHECK(server.wait_closed_by_client(std::chrono::seconds(5)));
    CHECK_FALSE(client.has_connected());
  }

  TEST_CASE("body_read_error_with_writes_in_flight_then_reconnect") {
    // The first big message cannot fit into the client's send buffer
    // (tcp_wmem maximum 4 MiB by default) plus the server's small receive
    // buffer, so its write stays in progress while the server does not read,
    // and the second big message waits in the queue.
    const std::string big(9 * 1024 * 1024, 'b');
    fake_server server(64 * 1024);
    rpc_client client;
    // The error callback runs after the client has closed its socket; the
    // test reconnects only after it, from this thread.
    std::atomic<bool> errored{false};
    client.set_error_callback([&errored](asio::error_code) { errored = true; });
    client.async_connect("127.0.0.1", server.port());
    REQUIRE(server.accept());
    REQUIRE(wait_until([&client] { return client.has_connected(); },
                       std::chrono::seconds(5)));

    auto call = async_add(client);
    rpc_header request{};
    REQUIRE(server.read_request(request));

    client.async_call<std::string>(
        "echo", [](asio::error_code, std::string) {}, big);
    client.async_call<std::string>(
        "echo", [](asio::error_code, std::string) {}, big);
    // Bytes of the first big message have arrived: its write has started.
    REQUIRE(wait_until([&server] { return server.available() > 0; },
                       std::chrono::seconds(5)));

    server.reply_partially(request, 100, 10);
    CHECK(wait_until([&call] { return call->done.load(); },
                     std::chrono::seconds(5)));
    CHECK(call->failed);
    REQUIRE(wait_until([&errored] { return errored.load(); },
                       std::chrono::seconds(5)));
    CHECK_FALSE(client.has_connected());

    // A later connection must not inherit the old queue.
    rpc_server next(0, 1, 15, 60);
    next.register_handler("add", add);
    REQUIRE(!next.async_run());
    client.update_addr("127.0.0.1", next.bound_port());
    client.async_connect("127.0.0.1", next.bound_port());
    REQUIRE(wait_until([&client] { return client.has_connected(); },
                       std::chrono::seconds(5)));
    auto later = async_add(client);
    CHECK(wait_until([&later] { return later->done.load(); },
                     std::chrono::seconds(5)));
    CHECK_FALSE(later->failed);
  }
}
