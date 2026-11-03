#include "doctest/doctest.h"
#include <array>
#include <atomic>
#include <chrono>
#include <fstream>
#include <functional>
#include <memory>
#include <rest_rpc.hpp>
#include <string>
#include <thread>
#include <vector>

using namespace rest_rpc;
using namespace rpc_service;

// The client's read and write errors: it closes the connection, drops what it
// has queued, reports the error to the pending callbacks and ignores answers
// and completions that belong to requests it no longer knows.
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

// The largest send buffer that Linux grants a TCP socket (tcp_wmem maximum),
// or 0 when it cannot be read.
size_t tcp_send_buffer_max() {
  std::ifstream file("/proc/sys/net/ipv4/tcp_wmem");
  size_t min = 0, def = 0, max = 0;
  if (!(file >> min >> def >> max)) {
    return 0;
  }
  return max;
}

// Whether a write of a message of this size stays in progress while the peer,
// with this receive buffer, does not read: the message must not fit into the
// client's send buffer (tcp_wmem maximum, 4 MiB by default) plus the peer's
// receive buffer. The kernel doubles both sizes for its bookkeeping.
bool write_stays_in_progress(size_t message, int receive_buffer) {
  const size_t send_buffer = tcp_send_buffer_max();
  if (send_buffer == 0 || 2 * (send_buffer + receive_buffer) >= message) {
    MESSAGE("skipped: tcp_wmem maximum " << send_buffer
            << " is too large to keep a write in progress");
    return false;
  }
  return true;
}

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

  // Answers an "add" request with the sum 3.
  void reply_sum(const rpc_header &request) {
    auto body = msgpack_codec::pack_args(0, 3);
    rpc_header header{MAGIC_NUM, request_type::req_res,
                      static_cast<uint32_t>(body.size()), request.req_id,
                      request.func_id};
    asio::write(socket_, asio::buffer(&header, sizeof(header)));
    asio::write(socket_, asio::buffer(body.data(), body.size()));
  }

  // Answers the request with a header announcing body_len bytes and sends
  // only sent bytes of the body.
  void reply_body_part(const rpc_header &request, uint32_t body_len,
                       size_t sent) {
    rpc_header header{MAGIC_NUM, request_type::req_res, body_len,
                      request.req_id, request.func_id};
    std::vector<char> body(sent, 'x');
    asio::write(socket_, asio::buffer(&header, sizeof(header)));
    asio::write(socket_, asio::buffer(body));
  }

  // Closes the connection with a reset.
  void reset() {
    socket_.set_option(asio::socket_base::linger(true, 0));
    socket_.close();
  }

  // Answers the request with a header announcing body_len bytes, sends only
  // sent bytes of the body and shuts the sending side down.
  void reply_partially(const rpc_header &request, uint32_t body_len,
                       size_t sent) {
    reply_body_part(request, body_len, sent);
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
  std::atomic<int> calls{0};
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
        ++result->calls;
        result->done = true;
      },
      1, 2);
  return result;
}

// Waits until bytes have arrived and the server's receive buffer has stopped
// filling, which in practice means that the client's write waits in the
// reactor; it does not guarantee it. While the buffers still drain, the write
// may be between two partial writes, so that a reset would fail the pending
// read first.
bool wait_write_blocked(fake_server &server) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  size_t last = 0;
  int stable = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    const size_t now = server.available();
    stable = (now > 0 && now == last) ? stable + 1 : 0;
    if (stable == 10) {
      return true;
    }
    last = now;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

// A connected client and the fake server's end of its connection.
struct connected_pair {
  explicit connected_pair(int receive_buffer = 0) : server(receive_buffer) {}

  bool connect() {
    client.async_connect("127.0.0.1", server.port());
    return server.accept() &&
           wait_until([this] { return client.has_connected(); },
                      std::chrono::seconds(5));
  }

  fake_server server;
  rpc_client client;
};

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
    // The write of the first big message stays in progress while the server
    // does not read, and the second big message waits in the queue.
    const std::string big(9 * 1024 * 1024, 'b');
    const int receive_buffer = 64 * 1024;
    if (!write_stays_in_progress(big.size(), receive_buffer)) {
      return;
    }
    // The error callback runs after the client has closed its socket; the
    // test reconnects only after it, from this thread. Declared before the
    // client, which may still run the callback while it is destroyed.
    std::atomic<bool> errored{false};
    fake_server server(receive_buffer);
    rpc_client client;
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

  // A second answer to a request finds another request pending: it is
  // ignored, and the pending request still gets its own answer.
  TEST_CASE("repeated_answer_with_another_callback_pending") {
    connected_pair pair;
    REQUIRE(pair.connect());
    auto first = async_add(pair.client);
    auto second = async_add(pair.client);
    rpc_header first_request{}, second_request{};
    REQUIRE(pair.server.read_request(first_request));
    REQUIRE(pair.server.read_request(second_request));

    pair.server.reply_sum(first_request);
    pair.server.reply_sum(first_request);
    pair.server.reply_sum(second_request);
    CHECK(wait_until([&second] { return second->done.load(); },
                     std::chrono::seconds(5)));
    CHECK_FALSE(second->failed);
    CHECK(first->calls == 1);
    CHECK_FALSE(first->failed);
    CHECK(pair.client.has_connected());
  }

  TEST_CASE("repeated_answer_with_another_future_pending") {
    connected_pair pair;
    REQUIRE(pair.connect());
    auto first = pair.client.async_call<FUTURE>("add", 1, 2);
    auto second = pair.client.async_call<FUTURE>("add", 1, 2);
    rpc_header first_request{}, second_request{};
    REQUIRE(pair.server.read_request(first_request));
    REQUIRE(pair.server.read_request(second_request));

    pair.server.reply_sum(first_request);
    pair.server.reply_sum(first_request);
    pair.server.reply_sum(second_request);
    REQUIRE(first.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready);
    CHECK(first.get().as<int>() == 3);
    REQUIRE(second.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready);
    CHECK(second.get().as<int>() == 3);
    CHECK(pair.client.has_connected());
  }

  // The other requests pending at a body read error get the error as well.
  TEST_CASE("body_read_error_reports_to_the_other_callbacks") {
    connected_pair pair;
    REQUIRE(pair.connect());
    auto first = async_add(pair.client);
    auto second = async_add(pair.client);
    rpc_header first_request{}, second_request{};
    REQUIRE(pair.server.read_request(first_request));
    REQUIRE(pair.server.read_request(second_request));

    pair.server.reply_partially(first_request, 100, 10);
    CHECK(wait_until([&first] { return first->done.load(); },
                     std::chrono::seconds(5)));
    CHECK(first->failed);
    CHECK(wait_until([&second] { return second->done.load(); },
                     std::chrono::seconds(5)));
    CHECK(second->failed);
    CHECK(second->calls == 1);
    CHECK_FALSE(second->connected_in_callback);
  }

  // The server resets the connection while a write is in progress, and the
  // requests pending then get the error. When the write waits in the reactor,
  // Asio completes it before the pending read; has_connected() being false in
  // the callback shows that the write error reported it, not the head read.
  TEST_CASE("write_error_reports_to_the_pending_callbacks") {
    const std::string big(9 * 1024 * 1024, 'b');
    const int receive_buffer = 64 * 1024;
    if (!write_stays_in_progress(big.size(), receive_buffer)) {
      return;
    }
    connected_pair pair(receive_buffer);
    REQUIRE(pair.connect());
    auto pending = async_add(pair.client);
    rpc_header request{};
    REQUIRE(pair.server.read_request(request));
    pair.client.async_call<std::string>(
        "echo", [](asio::error_code, std::string) {}, big);
    REQUIRE(wait_write_blocked(pair.server));

    pair.server.reset();
    CHECK(wait_until([&pending] { return pending->done.load(); },
                     std::chrono::seconds(5)));
    CHECK(pending->failed);
    CHECK(pending->calls == 1);
    CHECK_FALSE(pending->connected_in_callback);
  }

  // As above, with a response body half read: the write error drops the
  // requests, the error callback starts a new one, and the body read
  // completes afterwards for a request that is gone. Timing-dependent: if the
  // client has not started reading the body yet, the test passes without a
  // late body read; the repeated answer tests cover the lookup itself.
  TEST_CASE("stale_body_read_after_a_write_error") {
    const std::string big(9 * 1024 * 1024, 'b');
    const int receive_buffer = 64 * 1024;
    if (!write_stays_in_progress(big.size(), receive_buffer)) {
      return;
    }
    // Declared before the client, which may still run the error callback
    // while it is destroyed.
    std::atomic<bool> errored{false};
    future_result<req_result> started{};
    connected_pair pair(receive_buffer);
    rpc_client &client = pair.client;
    client.set_error_callback([&](asio::error_code) {
      if (!errored.exchange(true)) {
        started = client.async_call<FUTURE>("add", 1, 2);
      }
    });
    REQUIRE(pair.connect());
    auto first = client.async_call<FUTURE>("add", 1, 2);
    rpc_header request{};
    REQUIRE(pair.server.read_request(request));
    client.async_call<FUTURE>("echo", big);
    REQUIRE(wait_write_blocked(pair.server));
    pair.server.reply_body_part(request, 100, 10);
    // Lets the client read the header and start reading the body.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pair.server.reset();
    REQUIRE(wait_until([&errored] { return errored.load(); },
                       std::chrono::seconds(5)));
    REQUIRE(first.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready);
    CHECK_THROWS_AS(first.get(), std::future_error);
    CHECK_FALSE(client.has_connected());
  }
}
