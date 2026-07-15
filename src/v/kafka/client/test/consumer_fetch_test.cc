// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/test/cluster_mock.h"
#include "kafka/client/test/utils.h"
#include "kafka/client/topic_cache.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/sync_group.h"
#include "model/fundamental.h"

#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

#include <functional>

using namespace kafka::client;
using namespace std::chrono_literals;

namespace {

std::optional<kafka::batch_reader>
make_record_set(model::offset offset, size_t count) {
    iobuf record_set;
    auto writer{kafka::protocol::encoder(record_set)};
    kafka::protocol::writer_serialize_batch(writer, make_batch(offset, count));
    return kafka::batch_reader{std::move(record_set)};
}

// Partition-response builders for scripting fetch outcomes.
kafka::fetch_response::partition_response
records_at(model::partition_id p, model::offset offset, size_t count) {
    return kafka::fetch_response::partition_response{
      .partition_index = p,
      .error_code = kafka::error_code::none,
      .high_watermark = model::offset{-1},
      .last_stable_offset = model::offset{-1},
      .log_start_offset = model::offset{0},
      .records = make_record_set(offset, count)};
}

kafka::fetch_response::partition_response
out_of_range(model::partition_id p, model::offset log_start) {
    return kafka::fetch_response::partition_response{
      .partition_index = p,
      .error_code = kafka::error_code::offset_out_of_range,
      .high_watermark = model::offset{-1},
      .last_stable_offset = model::offset{-1},
      .log_start_offset = log_start,
      .records = std::nullopt};
}

kafka::fetch_response::partition_response
empty_partition(model::partition_id p) {
    return kafka::fetch_response::partition_response{
      .partition_index = p,
      .error_code = kafka::error_code::none,
      .high_watermark = model::offset{-1},
      .last_stable_offset = model::offset{-1},
      .log_start_offset = model::offset{0},
      .records = std::nullopt};
}

// Minimal in-memory group coordinator + leader over cluster_mock: enough of the
// consumer-group protocol (join/sync/heartbeat/offset_fetch/fetch) to drive a
// real consumer::fetch() against injectable committed offsets, errors, and
// scripted per-call fetch outcomes.
struct group_mock {
    // committed offset per partition (absent -> no committed offset / -1).
    absl::flat_hash_map<model::partition_id, model::offset> committed;
    // if set, OffsetFetch returns this per-partition error.
    std::optional<kafka::error_code> offset_fetch_error;
    // brokers whose Fetch returns a top-level error (-> dispatch failure).
    absl::flat_hash_set<model::node_id> fail_fetch;
    // per-partition fetch outcome; call_no is the index of the Fetch RPC, so a
    // test can script a sequence (e.g. out-of-range then records). Defaults to
    // records at the requested offset.
    std::function<kafka::fetch_response::partition_response(
      model::partition_id, model::offset, int call_no)>
      fetch_partition = [](model::partition_id p, model::offset o, int) {
          return records_at(p, o, 5);
      };

    // observed
    absl::flat_hash_map<model::partition_id, model::offset> last_fetch_offset;
    int offset_fetch_calls{0};
    int fetch_calls{0};

    void install(cluster_mock& mock) {
        mock.register_handler(
          kafka::join_group_api::key,
          [](model::node_id, request_t req, kafka::api_version) {
              auto r = std::get<kafka::join_group_request>(std::move(req));
              kafka::join_group_response resp;
              resp.data.error_code = kafka::error_code::none;
              resp.data.generation_id = kafka::generation_id{1};
              resp.data.member_id = kafka::member_id{"m1"};
              resp.data.leader = kafka::member_id{"m1"};
              resp.data.protocol_name = r.data.protocols.empty()
                                          ? kafka::protocol_name{"range"}
                                          : r.data.protocols[0].name;
              kafka::join_group_response_member m;
              m.member_id = kafka::member_id{"m1"};
              m.metadata = r.data.protocols.empty()
                             ? bytes{}
                             : std::move(r.data.protocols[0].metadata);
              resp.data.members.push_back(std::move(m));
              return ss::make_ready_future<response_t>(std::move(resp));
          });

        mock.register_handler(
          kafka::sync_group_api::key,
          [](model::node_id, request_t req, kafka::api_version) {
              auto r = std::get<kafka::sync_group_request>(std::move(req));
              kafka::sync_group_response resp;
              resp.data.error_code = kafka::error_code::none;
              for (auto& a : r.data.assignments) {
                  if (a.member_id == r.data.member_id) {
                      resp.data.assignment = std::move(a.assignment);
                      break;
                  }
              }
              return ss::make_ready_future<response_t>(std::move(resp));
          });

        mock.register_handler(
          kafka::heartbeat_api::key,
          [](model::node_id, request_t, kafka::api_version) {
              kafka::heartbeat_response resp;
              resp.data.error_code = kafka::error_code::none;
              return ss::make_ready_future<response_t>(std::move(resp));
          });

        mock.register_handler(
          kafka::offset_fetch_api::key,
          [this](model::node_id, request_t req, kafka::api_version) {
              ++offset_fetch_calls;
              auto r = std::get<kafka::offset_fetch_request>(std::move(req));
              kafka::offset_fetch_response resp;
              resp.data.error_code = kafka::error_code::none;
              for (const auto& t : *r.data.topics) {
                  kafka::offset_fetch_response_topic rt;
                  rt.name = t.name;
                  for (auto p : t.partition_indexes) {
                      kafka::offset_fetch_response_partition rp;
                      rp.partition_index = p;
                      rp.metadata = "";
                      if (offset_fetch_error) {
                          rp.committed_offset = model::offset{-1};
                          rp.error_code = *offset_fetch_error;
                      } else {
                          auto it = committed.find(p);
                          rp.committed_offset = it == committed.end()
                                                  ? model::offset{-1}
                                                  : it->second;
                          rp.error_code = kafka::error_code::none;
                      }
                      rt.partitions.push_back(std::move(rp));
                  }
                  resp.data.topics.push_back(std::move(rt));
              }
              return ss::make_ready_future<response_t>(std::move(resp));
          });

        mock.register_handler(
          kafka::fetch_api::key,
          [this](model::node_id node, request_t req, kafka::api_version) {
              auto r = std::get<kafka::fetch_request>(std::move(req));
              const int call = fetch_calls++;
              kafka::fetch_response resp;
              resp.data.session_id = kafka::invalid_fetch_session_id;
              if (fail_fetch.contains(node)) {
                  // top-level error -> dispatch_fetch throws -> the round is a
                  // dispatch failure for the consumer.
                  resp.data.error_code
                    = kafka::error_code::not_leader_for_partition;
                  return ss::make_ready_future<response_t>(std::move(resp));
              }
              resp.data.error_code = kafka::error_code::none;
              for (auto& t : r.data.topics) {
                  kafka::fetch_response::partition p{.topic = t.topic};
                  for (auto& pp : t.partitions) {
                      last_fetch_offset[pp.partition] = pp.fetch_offset;
                      p.partitions.push_back(
                        fetch_partition(pp.partition, pp.fetch_offset, call));
                  }
                  resp.data.responses.push_back(std::move(p));
              }
              return ss::make_ready_future<response_t>(std::move(resp));
          });

        mock.register_handler(
          kafka::leave_group_api::key,
          [](model::node_id, request_t, kafka::api_version) {
              kafka::leave_group_response resp;
              resp.data.error_code = kafka::error_code::none;
              return ss::make_ready_future<response_t>(std::move(resp));
          });
    }
};

struct consumer_fetch_fixture : public ::testing::Test {
    const model::topic topic{"t"};

    cluster_mock mock;
    ss::logger log{"consumer-fetch-test"};
    prefix_logger plog{log, "test"};
    brokers cluster_brokers{plog, std::make_unique<broker_mock_factory>(&mock)};
    topic_cache tc;

    consumer_configuration cfg{
      .request_timeout = 1s,
      .fetch_min_bytes = 1,
      .fetch_max_bytes = 1024 * 1024,
      .session_timeout = 30s,
      .rebalance_timeout = 30s,
      .heartbeat_interval = 3s};
    retries_configuration retries{.max_retries = 5, .retry_base_backoff = 10ms};

    // n_brokers brokers (node ids 1..n) and one topic with n_partitions.
    // add_topic assigns partition p to leader 1 + (p % n_brokers); topic_cache
    // is populated to match.
    void setup(int n_brokers = 1, int n_partitions = 1) {
        mock.register_default_handlers();
        chunked_vector<metadata_update::broker> bs;
        for (int n = 1; n <= n_brokers; ++n) {
            mock.add_broker(
              model::node_id{n},
              net::unresolved_address{"localhost", uint16_t(9092 + n)});
            bs.push_back(
              metadata_update::broker{
                .node_id = model::node_id{n},
                .host = "localhost",
                .port = 9092 + n});
        }
        cluster_brokers.apply(bs).get();
        mock.add_topic(topic, n_partitions, 1);

        chunked_vector<kafka::metadata_response::topic> topics;
        kafka::metadata_response::topic mt;
        mt.name = topic;
        for (int p = 0; p < n_partitions; ++p) {
            mt.partitions.push_back(
              kafka::metadata_response::partition{
                .partition_index = model::partition_id{p},
                .leader_id = model::node_id{1 + (p % n_brokers)}});
        }
        topics.push_back(std::move(mt));
        tc.apply(topics);
    }

    shared_consumer_t make(group_mock& g) {
        g.install(mock);
        auto coordinator = cluster_brokers.find(model::node_id{1});
        return make_consumer(
                 cfg,
                 retries,
                 tc,
                 cluster_brokers,
                 coordinator,
                 kafka::group_id{"g"},
                 kafka::no_member,
                 [](const kafka::member_id&) {},
                 [](std::exception_ptr) { return ss::now(); },
                 plog)
          .get();
    }

    void TearDown() override { cluster_brokers.stop().get(); }
};

bool has_records(const kafka::fetch_response& res) {
    for (auto& t : res.data.responses) {
        for (auto& p : t.partitions) {
            if (p.records && !p.records->empty()) {
                return true;
            }
        }
    }
    return false;
}

} // namespace

// Resumes from committed+1 (our commit stores the last consumed offset).
TEST_F(consumer_fetch_fixture, ResumesFromCommittedOffset) {
    setup();
    group_mock g;
    g.committed[model::partition_id{0}] = model::offset{9};
    auto c = make(g);

    c->subscribe({topic}).get();
    auto res = c->fetch(1s, std::nullopt).get();

    EXPECT_EQ(g.last_fetch_offset[model::partition_id{0}], model::offset{10});
    EXPECT_TRUE(has_records(res));
    c->leave().get();
}

// No committed offset -> earliest (0).
TEST_F(consumer_fetch_fixture, NoCommittedStartsAtEarliest) {
    setup();
    group_mock g;
    auto c = make(g);

    c->subscribe({topic}).get();
    c->fetch(1s, std::nullopt).get();

    EXPECT_EQ(g.last_fetch_offset[model::partition_id{0}], model::offset{0});
    c->leave().get();
}

// A per-partition OffsetFetch error surfaces (throws) rather than silently
// starting at earliest.
TEST_F(consumer_fetch_fixture, PartitionErrorOnOffsetFetchThrows) {
    setup();
    group_mock g;
    g.offset_fetch_error = kafka::error_code::unknown_topic_or_partition;
    auto c = make(g);

    c->subscribe({topic}).get();
    EXPECT_THROW(c->fetch(1s, std::nullopt).get(), partition_error);
    c->leave().get();
}

// Committed below the (retention-trimmed) log start: seed committed+1, the
// fetch hits offset_out_of_range, and recovery reseeds to the log start.
TEST_F(consumer_fetch_fixture, CommittedBelowLogStartRecoversToLogStart) {
    setup();
    group_mock g;
    g.committed[model::partition_id{0}] = model::offset{5};
    g.fetch_partition = [](model::partition_id p, model::offset o, int call) {
        // First round (fetch_offset 6) is out of range; after reseed to 30 the
        // second round returns records.
        return call == 0 ? out_of_range(p, model::offset{30})
                         : records_at(p, o, 5);
    };
    auto c = make(g);

    c->subscribe({topic}).get();
    auto res = c->fetch(1s, std::nullopt).get();

    EXPECT_TRUE(has_records(res));
    EXPECT_EQ(g.last_fetch_offset[model::partition_id{0}], model::offset{30});
    c->leave().get();
}

// offset_out_of_range recovers within a single poll (the in-poll loop reseeds
// and refetches before returning), not on a later poll.
TEST_F(consumer_fetch_fixture, OutOfRangeRecoversInSinglePoll) {
    setup();
    group_mock g;
    g.fetch_partition = [](model::partition_id p, model::offset o, int call) {
        return call == 0 ? out_of_range(p, model::offset{30})
                         : records_at(p, o, 5);
    };
    auto c = make(g);

    c->subscribe({topic}).get();
    auto res = c->fetch(1s, std::nullopt).get();

    EXPECT_TRUE(has_records(res));
    c->leave().get();
}

// An empty long-poll (no records, no error, no reseed) returns empty.
TEST_F(consumer_fetch_fixture, EmptyPollReturnsEmpty) {
    setup();
    group_mock g;
    g.fetch_partition = [](model::partition_id p, model::offset, int) {
        return empty_partition(p);
    };
    auto c = make(g);

    c->subscribe({topic}).get();
    auto res = c->fetch(1s, std::nullopt).get();

    EXPECT_FALSE(has_records(res));
    c->leave().get();
}

// Fast path: once every assigned partition has a position, a later fetch issues
// no OffsetFetch.
TEST_F(consumer_fetch_fixture, NoOffsetFetchOncePositioned) {
    setup();
    group_mock g;
    auto c = make(g);

    c->subscribe({topic}).get();
    c->fetch(1s, std::nullopt).get();
    const int after_first = g.offset_fetch_calls;
    c->fetch(1s, std::nullopt).get();

    EXPECT_EQ(g.offset_fetch_calls, after_first);
    c->leave().get();
}

// A dispatch failure on one broker discards the whole round and throws; the
// healthy sibling's offset must NOT advance, so the retry re-reads it from the
// same offset (no silent data loss).
TEST_F(consumer_fetch_fixture, DispatchFailureDoesNotAdvanceHealthySibling) {
    setup(/*n_brokers=*/2, /*n_partitions=*/2);
    group_mock g;
    g.fail_fetch.insert(model::node_id{2}); // partition 1's leader fails
    auto c = make(g);

    c->subscribe({topic}).get();
    EXPECT_THROW(c->fetch(1s, std::nullopt).get(), broker_error);

    // Recover: clear the failure and fetch again. The healthy partition 0 is
    // re-read from 0 -- the discarded round did not advance past its records.
    g.fail_fetch.clear();
    auto res = c->fetch(1s, std::nullopt).get();
    EXPECT_EQ(g.last_fetch_offset[model::partition_id{0}], model::offset{0});
    EXPECT_TRUE(has_records(res));
    c->leave().get();
}
