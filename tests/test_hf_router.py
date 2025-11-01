"""Smoke test for HF Router integration"""
import os
import sys
sys.path.append('.')

from api.inference import _call_llm

def test_hf_router_basic():
    """Test that HF Router can answer a simple question"""
    try:
        # Set required env vars if not present
        if not os.getenv("HF_TOKEN"):
            print("⚠️  HF_TOKEN not set, skipping HF Router test")
            return True
            
        response = _call_llm("What is the capital of France?")
        print(f"HF Router response: {response}")
        
        # Basic sanity check
        assert "paris" in response.lower(), f"Expected 'Paris' in response, got: {response}"
        print("✅ HF Router test passed")
        return True
        
    except Exception as e:
        print(f"❌ HF Router test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_hf_router_basic()
    sys.exit(0 if success else 1)
