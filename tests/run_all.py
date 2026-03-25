import json
import os
import sys
import sqlite3

# Add app to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'app')))
from sql_validator import validate_sql
from nl_to_sql import convert_to_sql

def run_tests():
    print("🚀 Running Marketplace Intelligence Test Suite")
    print("-" * 50)
    
    # 1. SQL Safety Tests
    with open("tests/fixtures/sql_safety_cases.json", "r") as f:
        safety_cases = json.load(f)
    
    safety_passed = 0
    for case in safety_cases:
        valid, result = validate_sql(case["sql"])
        if valid == case["expected_valid"]:
            safety_passed += 1
        else:
            print(f"❌ Safety Failed: {case['sql'][:50]}... Expected {case['expected_valid']}, got {valid} ({result})")
    
    print(f"🛡️ SQL Safety Gate: {safety_passed}/{len(safety_cases)} passed")

    # 2. NL Query Tests (Rule-based)
    with open("tests/fixtures/nl_query_cases.json", "r") as f:
        nl_cases = json.load(f)
    
    nl_passed = 0
    for case in nl_cases:
        res = convert_to_sql(case["question"])
        sql = res["sql"].upper()
        if all(kw.upper() in sql for kw in case["expected_keywords"]):
            nl_passed += 1
        else:
            print(f"❌ NL Accuracy Failed: '{case['question']}'")
            print(f"   Generated: {res['sql']}")
    
    print(f"📊 NL Query Accuracy: {nl_passed}/{len(nl_cases)} passed")
    print("-" * 50)
    
    total = safety_passed + nl_passed
    max_total = len(safety_cases) + len(nl_cases)
    print(f"🏁 Overall: {total}/{max_total} ({total/max_total*100:.1f}%)")
    
    if safety_passed == len(safety_cases):
        print("SQL Safety Gate: ALL PASSED ✅")
    else:
        print("SQL Safety Gate: FAILED ❌")

if __name__ == "__main__":
    run_tests()
