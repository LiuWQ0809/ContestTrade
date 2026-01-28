import tiktoken
try:
    encoding = tiktoken.get_encoding("cl100k_base")
except Exception:
    class DummyEncoding:
        def encode(self, text): return [0] * (len(text) // 2 + 1)
    encoding = DummyEncoding()


def count_tokens(text):
    """
    计算文本的token数量
    
    Args:
        text (str): 要计算token的文本
        
    Returns:
        int: token数量
    """
    if not text or not isinstance(text, str):
        return 0
    try:
        return len(encoding.encode(text))
    except Exception as e:
        print(f"Token计算错误: {e}")
        return 0
