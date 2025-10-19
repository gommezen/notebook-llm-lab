from ollama import chat

#def print_llm_response(prompt):
    #response = chat(model="phi3:mini", messages=[{"role": "user", "content": prompt}])
    #print(response['message']['content'])

def print_llm_response(prompt):
    response = chat(model="deepseek-coder:1.3b", messages=[{"role": "user", "content": prompt}])
    print(response['message']['content'])

if __name__ == "__main__":
    print_llm_response("Summarize the history of Linux in one paragraph.")
