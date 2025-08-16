#include "manusya/bank.h"
#include "manusya/mem_store.h"

#include <gtest/gtest.h>

namespace {
using namespace pain::manusya;

TEST(Bank, Basic) {
    auto mem_story = Store::create("memory://");
    ASSERT_TRUE(mem_story != nullptr);

    Bank bank(mem_story);
    bank.load();

    ChunkOptions options;
    ChunkPtr chunk;
    bank.create_chunk(options, &chunk);
    ASSERT_TRUE(chunk != nullptr);
}

} // namespace
