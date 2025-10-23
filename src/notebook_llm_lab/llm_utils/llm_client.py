"""
Minimal helper for interacting with a local Ollama LLM.

This version is designed for interactive notebooks or small experiments:
it prints the model's reply for quick inspection and also returns the text
so you can reuse it programmatically (e.g., store it, analyze it, or feed it
into another step).
"""

import os
from typing import Any

from ollama import chat

DEFAULT_MODEL = os.getenv("OLLAMA_DEFAULT_MODEL", "qwen2.5:7b-instruct-q4_0")


def ask_model(prompt: str, model: str = DEFAULT_MODEL, verbose: bool = True) -> str:
    """
    Send a prompt to a local Ollama model and return the assistant's reply.
    """
    try:
        response: Any = chat(
            model=model, messages=[{"role": "user", "content": prompt}]
        )
        text = response["message"]["content"].strip()
    except Exception as e:
        text = f"[Error querying model '{model}': {e}]"

    if verbose:
        print(text)

    # Return the same text so you can use it programmatically:
    #   answer = ask_model("Explain PCA")
    #   print(len(answer))  # or process it further
    return text
