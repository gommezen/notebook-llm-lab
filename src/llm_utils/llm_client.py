"""
Minimal helper for interacting with a local Ollama LLM.

This version is designed for interactive notebooks or small experiments:
it prints the model's reply for quick inspection and also returns the text
so you can reuse it programmatically (e.g., store it, analyze it, or feed it
into another step).
"""

from ollama import chat


def ask_model(prompt: str, model: str = "phi3:mini") -> str:
    """
    Send a user prompt to a local Ollama model and return its textual reply.

    Parameters
    ----------
    prompt : str
        The text prompt to send to the model.
    model : str, optional
        The local model to use (e.g. "phi3:mini" or "deepseek-coder:1.3b").
        Defaults to "phi3:mini".

    Returns
    -------
    str
        The assistant's textual reply extracted from the Ollama response.
    """

    # Send the prompt to Ollama's local chat endpoint.
    # 'messages' is a list of conversation turns; we send just one user message.
    response = chat(model=model, messages=[{"role": "user", "content": prompt}])

    # Extract the text content from the model's reply.
    text = response["message"]["content"]

    # Print for immediate feedback in notebooks or terminals.
    print(text)

    # Return the same text so you can use it programmatically:
    #   answer = ask_model("Explain PCA")
    #   print(len(answer))  # or process it further
    return text