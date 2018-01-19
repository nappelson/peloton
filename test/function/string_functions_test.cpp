//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions_test.cpp
//
// Identification: test/expression/string_functions_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include <string>
#include <vector>

#include "common/harness.h"

#include "executor/executor_context.h"
#include "function/string_functions.h"
#include "function/old_engine_string_functions.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class StringFunctionsTests : public PelotonTest {
 public:
  StringFunctionsTests() : test_ctx_(nullptr) {}

  executor::ExecutorContext &GetExecutorContext() { return test_ctx_; }

 private:
  executor::ExecutorContext test_ctx_;
};

TEST_F(StringFunctionsTests, LikeTest) {
  //-------------- match ---------------- //
  std::string s1 = "forbes \\avenue";  // "forbes \avenue"
  std::string p1 = "%b_s \\\\avenue";  // "%b_s \\avenue"
  EXPECT_TRUE(function::StringFunctions::Like(
      GetExecutorContext(), s1.c_str(), s1.size(), p1.c_str(), p1.size()));

  std::string s2 = "for%bes avenue%";    // "for%bes avenue%"
  std::string p2 = "for%bes a_enue\\%";  // "for%bes a_enue%"
  EXPECT_TRUE(function::StringFunctions::Like(
      GetExecutorContext(), s2.c_str(), s2.size(), p2.c_str(), p2.size()));

  std::string s3 = "Allison";  // "Allison"
  std::string p3 = "%lison";   // "%lison"
  EXPECT_TRUE(function::StringFunctions::Like(
      GetExecutorContext(), s3.c_str(), s3.size(), p3.c_str(), p3.size()));

  //----------Exact Match------------//
  std::string s5 = "Allison";  // "Allison"
  std::string p5 = "Allison";  // "Allison"
  EXPECT_TRUE(function::StringFunctions::Like(
      GetExecutorContext(), s5.c_str(), s5.size(), p5.c_str(), p5.size()));

  //----------Exact Match------------//
  std::string s6 = "Allison";   // "Allison"
  std::string p6 = "A%llison";  // "A%llison"
  EXPECT_TRUE(function::StringFunctions::Like(
      GetExecutorContext(), s6.c_str(), s6.size(), p6.c_str(), p6.size()));

  //-------------- not match ----------------//
  std::string s4 = "forbes avenue";  // "forbes avenue"
  std::string p4 = "f_bes avenue";   // "f_bes avenue"
  EXPECT_FALSE(function::StringFunctions::Like(
      GetExecutorContext(), s4.c_str(), s4.size(), p4.c_str(), p4.size()));
}

TEST_F(StringFunctionsTests, AsciiTest) {
  const char column_char = 'A';
  for (int i = 0; i < 52; i++) {
    auto expected = static_cast<uint32_t>(column_char + i);

    std::ostringstream os;
    os << static_cast<char>(expected);
    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue(os.str())};

    auto result = function::OldEngineStringFunctions::Ascii(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};
  auto result = function::OldEngineStringFunctions::Ascii(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, ChrTest) {
  const char column_char = 'A';
  for (int i = 0; i < 52; i++) {
    int char_int = (int)column_char + i;

    std::ostringstream os;
    os << static_cast<char>(char_int);
    std::string expected = os.str();

    std::vector<type::Value> args = {
        type::ValueFactory::GetIntegerValue(char_int)};

    auto result = function::OldEngineStringFunctions::Chr(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }

  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER)};
  auto result = function::OldEngineStringFunctions::Chr(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, SubstrTest) {
  std::vector<std::string> words = {"Fuck", "yo", "couch"};
  std::ostringstream os;
  for (auto w : words) {
    os << w;
  }
  int from = words[0].size() + 1;
  int len = words[1].size();
  const std::string expected = words[1];
  std::vector<type::Value> args = {
      type::ValueFactory::GetVarcharValue(os.str()),
      type::ValueFactory::GetIntegerValue(from),
      type::ValueFactory::GetIntegerValue(len),
  };
  auto result = function::OldEngineStringFunctions::Substr(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 3; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
        type::ValueFactory::GetVarcharValue("ccc"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::OldEngineStringFunctions::Substr(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, CharLengthTest) {
  const std::string str = "A";
  for (int i = 0; i < 100; i++) {
    std::string input = StringUtil::Repeat(str, i);
    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue(input)};

    auto result = function::OldEngineStringFunctions::CharLength(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(i, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};
  auto result = function::OldEngineStringFunctions::CharLength(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, RepeatTest) {
  const std::string str = "A";
  for (int i = 0; i < 100; i++) {
    std::string expected = StringUtil::Repeat(str, i);
    EXPECT_EQ(i, expected.size());
    std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(str),
                                     type::ValueFactory::GetIntegerValue(i)};

    auto result = function::OldEngineStringFunctions::Repeat(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR),
      type::ValueFactory::GetVarcharValue(str),
  };
  auto result = function::OldEngineStringFunctions::Repeat(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, ReplaceTest) {
  const std::string origChar = "A";
  const std::string replaceChar = "X";
  const std::string prefix = "**PAVLO**";
  for (int i = 0; i < 100; i++) {
    std::string expected = prefix + StringUtil::Repeat(origChar, i);
    EXPECT_EQ(i + prefix.size(), expected.size());
    std::string input = prefix + StringUtil::Repeat(replaceChar, i);
    EXPECT_EQ(i + prefix.size(), expected.size());

    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue(input),
        type::ValueFactory::GetVarcharValue(replaceChar),
        type::ValueFactory::GetVarcharValue(origChar)};

    auto result = function::OldEngineStringFunctions::Replace(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }
  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 3; i++) {
    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
        type::ValueFactory::GetVarcharValue("ccc"),
    };
    args[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::OldEngineStringFunctions::Replace(args);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, LTrimTest) {
  const std::string message = "This is a string with spaces";
  const std::string spaces = "    ";
  const std::string origStr = spaces + message + spaces;
  const std::string expected = message + spaces;
  std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(origStr),
                                   type::ValueFactory::GetVarcharValue(" ")};
  auto result = function::OldEngineStringFunctions::LTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::OldEngineStringFunctions::LTrim(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, RTrimTest) {
  const std::string message = "This is a string with spaces";
  const std::string spaces = "    ";
  const std::string origStr = spaces + message + spaces;
  const std::string expected = spaces + message;
  std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(origStr),
                                   type::ValueFactory::GetVarcharValue(" ")};
  auto result = function::OldEngineStringFunctions::RTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::OldEngineStringFunctions::RTrim(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, BTrimTest) {
  const std::string message = "This is a string with spaces";
  const std::string spaces = "    ";
  const std::string origStr = spaces + message + spaces;
  const std::string expected = message;
  std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(origStr),
                                   type::ValueFactory::GetVarcharValue(" ")};
  auto result = function::OldEngineStringFunctions::BTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  result = function::OldEngineStringFunctions::Trim(
      {type::ValueFactory::GetVarcharValue(origStr)});
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::OldEngineStringFunctions::BTrim(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, LengthTest) {
  const char column_char = 'A';
  int expected = 1;
  std::string str = "";
  for (int i = 0; i < 52; i++) {
    str += (char)(column_char + i);
    expected++;

    std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(str)};

    auto result = function::OldEngineStringFunctions::Length(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};
  auto result = function::OldEngineStringFunctions::Length(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, UpperTest) {
  const std::string s1 = "abc";
  const std::string s1_upper = StringUtil::Upper(s1);
  EXPECT_STREQ(function::StringFunctions::Upper(
      GetExecutorContext(), s1.c_str(), s1.size()), s1_upper.c_str());

  const std::string s2 = "";
  const std::string s2_upper = StringUtil::Upper(s2);
  EXPECT_STREQ(function::StringFunctions::Upper(
      GetExecutorContext(), s2.c_str(), s2.size()), s2_upper.c_str());

  const std::string s3 = "aBc";
  const std::string s3_upper = StringUtil::Upper(s3);
  EXPECT_STREQ(function::StringFunctions::Upper(
      GetExecutorContext(), s3.c_str(), s3.size()), s3_upper.c_str());
}

TEST_F(StringFunctionsTests, LowerTest) {
  const std::string s1 = "ABC";
  const std::string s1_lower = StringUtil::Lower(s1);
  EXPECT_STREQ(function::StringFunctions::Lower(
      GetExecutorContext(), s1.c_str(), s1.size()), s1_lower.c_str());

  const std::string s2 = "";
  const std::string s2_lower = StringUtil::Lower(s2);
  EXPECT_STREQ(function::StringFunctions::Lower(
      GetExecutorContext(), s2.c_str(), s2.size()), s2_lower.c_str());

  const std::string s3 = "AbC";
  const std::string s3_lower = StringUtil::Lower(s3);
  EXPECT_STREQ(function::StringFunctions::Lower(
      GetExecutorContext(), s3.c_str(), s3.size()), s3_lower.c_str());
}

TEST_F(StringFunctionsTests, ConcatTest) {
  const char *strings1[] = {
      "A", "B", "C"
  };
  uint32_t lengths1[] = {1, 1, 1};
  const std::string concat_1 = "ABC";
  auto result1 = function::StringFunctions::Concat(
      GetExecutorContext(), strings1, lengths1, 3);
  EXPECT_STREQ(concat_1.c_str(), result1.str);

  const char *strings2[] = {
      "", "BCD", "X"
  };
  uint32_t lengths2[] = {0, 3, 1};
  const std::string concat_2 = "BCDX";
  auto result2 = function::StringFunctions::Concat(
      GetExecutorContext(), strings2, lengths2, 3);
  EXPECT_STREQ(concat_2.c_str(), result2.str);
}

TEST_F(StringFunctionsTests, CodegenSubstrTest) {
  const std::string message = "1234567";
  int from = 1;
  int len = 5;
  std::string expected = message.substr(from - 1, len);
  auto res = function::StringFunctions::Substr(
      GetExecutorContext(), message.c_str(), message.length(), from, len);
  EXPECT_EQ(len + 1, res.length);
  EXPECT_EQ(expected, std::string(res.str, len));

  from = 7;
  len = 1;
  expected = message.substr(from - 1, len);
  res = function::StringFunctions::Substr(GetExecutorContext(), message.c_str(),
                                          message.length(), from, len);
  EXPECT_EQ(len + 1, res.length);
  EXPECT_EQ(expected, std::string(res.str, len));

  from = -2;
  len = 4;
  expected = message.substr(0, 1);
  res = function::StringFunctions::Substr(GetExecutorContext(), message.c_str(),
                                          message.length(), from, len);
  EXPECT_EQ(2, res.length);
  EXPECT_EQ(expected, std::string(res.str, 1));

  from = -2;
  len = 2;
  expected = "";
  res = function::StringFunctions::Substr(GetExecutorContext(), message.c_str(),
                                          message.length(), from, len);
  EXPECT_EQ(0, res.length);
  EXPECT_EQ(nullptr, res.str);
}


}  // namespace test
}  // namespace peloton
