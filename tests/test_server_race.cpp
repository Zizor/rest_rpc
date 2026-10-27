#include "doctest/doctest.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <rest_rpc.hpp>
#include <thread>
#include <vector>

using namespace rest_rpc;
using namespace rpc_service;

// Server start, stop and connection close racing the server's clean() thread.
// check_seconds is 0 so that clean() scans without pause; these cases are
// meant to run under ThreadSanitizer.
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

// rpc_client::connect() is not used: its wait has a race of its own.
std::unique_ptr<rpc_client> connect_client(unsigned short port) {
  std::unique_ptr<rpc_client> client(new rpc_client);
  client->async_connect("127.0.0.1", port);
  REQUIRE(wait_until([&client] { return client->has_connected(); },
                     std::chrono::seconds(5)));
  return client;
}

} // namespace

TEST_SUITE("server_race") {
  TEST_CASE("server_start_stop_while_cleaning") {
    for (int i = 0; i < 300; ++i) {
      rpc_server server(0, 1, 15, 0);
      REQUIRE(!server.async_run());
    }
  }

  TEST_CASE("connection_closed_by_peer_while_cleaning") {
    rpc_server server(0, 1, 60, 0);
    std::atomic<int> cleaned{0};
    server.set_conn_timeout_callback([&cleaned](int64_t) { ++cleaned; });
    REQUIRE(!server.async_run());
    const unsigned short port = server.bound_port();

    const int count = 50;
    for (int i = 0; i < count; ++i) {
      connect_client(port);
    }
    CHECK(wait_until([&cleaned] { return cleaned.load() == count; },
                     std::chrono::seconds(10)));
  }

  TEST_CASE("connection_idle_timeout_while_cleaning") {
    rpc_server server(0, 1, 1, 0);
    std::atomic<int> cleaned{0};
    server.set_conn_timeout_callback([&cleaned](int64_t) { ++cleaned; });
    REQUIRE(!server.async_run());
    const unsigned short port = server.bound_port();

    const int count = 5;
    std::vector<std::unique_ptr<rpc_client>> clients;
    for (int i = 0; i < count; ++i) {
      clients.push_back(connect_client(port));
    }
    CHECK(wait_until([&cleaned] { return cleaned.load() == count; },
                     std::chrono::seconds(10)));
  }
}
