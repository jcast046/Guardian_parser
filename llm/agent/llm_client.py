"""LLM client supporting Ollama and llama.cpp backends.

Provides unified interface for interacting with different LLM backends
for JSON-structured extraction and summarization tasks.
"""
import json
import os
from typing import List, Dict, Any, Optional
from pathlib import Path


class LLMClient:
    """LLM client supporting Ollama and llama.cpp backends.

    Provides unified interface for JSON-structured LLM interactions with
    support for both HTTP-based (Ollama) and local (llama.cpp) backends.
    """
    
    def __init__(
        self,
        backend: str = "ollama",
        model_path: Optional[str] = None,
        ollama_model: str = "llama3.2",
        temperature: float = 0.1,
        json_mode: bool = True
    ):
        """Initialize LLM client.

        Args:
            backend: Backend to use ("ollama" or "llama").
            model_path: Path to GGUF model file (required for llama backend).
            ollama_model: Ollama model name (required for ollama backend).
            temperature: Sampling temperature for generation.
            json_mode: Whether to request JSON-formatted responses.

        Raises:
            ImportError: If required backend dependencies are not installed.
            FileNotFoundError: If model file not found for llama backend.
        """
        self.backend = backend  # "ollama" | "llama"
        self.model_path = model_path
        self.ollama_model = ollama_model
        self.temperature = temperature
        self.json_mode = json_mode
        self._llm = None
        self._init_backend()
    
    def _init_backend(self):
        """Initialize the selected backend.

        Raises:
            ImportError: If backend dependencies are not installed.
            FileNotFoundError: If model file not found (llama backend).
        """
        if self.backend == "llama":
            try:
                from llama_cpp import Llama
                
                # If no model_path provided, look for GGUF in models directory
                if not self.model_path:
                    root_dir = Path(__file__).parent.parent.parent.resolve()
                    model_dir = root_dir / "models" / "Llama3_2-3B-Instruct"
                    # Look for .gguf files
                    gguf_files = list(model_dir.glob("*.gguf"))
                    if gguf_files:
                        self.model_path = str(gguf_files[0])
                    else:
                        raise FileNotFoundError(
                            f"No GGUF file found in {model_dir}. "
                            "Please convert your safetensors to GGUF or specify --model-path"
                        )
                
                if not os.path.exists(self.model_path):
                    raise FileNotFoundError(f"Model file not found: {self.model_path}")
                
                self._llm = Llama(
                    model_path=self.model_path,
                    n_ctx=8192,
                    n_gpu_layers=0,  # CPU only by default
                    verbose=False
                )
            except ImportError:
                raise ImportError(
                    "llama-cpp-python not installed. Install with: pip install llama-cpp-python"
                )
        else:  # ollama
            try:
                import requests
                self._requests = requests
            except ImportError:
                raise ImportError(
                    "requests not installed. Install with: pip install requests"
                )
    
    def chat_json(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """Send messages to LLM and get JSON response.

        Args:
            messages: List of message dictionaries with "role" and "content"
                keys following OpenAI chat format.

        Returns:
            Parsed JSON dictionary from LLM response.

        Raises:
            RuntimeError: If backend connection fails or response is invalid.
            ValueError: If response does not contain valid JSON.
        """
        if self.backend == "llama":
            return self._chat_llama(messages)
        else:  # ollama
            return self._chat_ollama(messages)
    
    def _chat_llama(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """Chat with llama.cpp backend.

        Args:
            messages: List of message dictionaries.

        Returns:
            Parsed JSON dictionary from response.

        Raises:
            ValueError: If response does not contain valid JSON.
        """
        # Convert messages to llama.cpp format
        formatted_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in messages
        ]
        
        # Create completion
        response_format = {"type": "json_object"} if self.json_mode else None
        completion = self._llm.create_chat_completion(
            messages=formatted_messages,
            temperature=self.temperature,
            response_format=response_format
        )
        
        content = completion["choices"][0]["message"]["content"]
        
        # Extract JSON from response
        return self._extract_json(content)
    
    def _chat_ollama(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """Chat with Ollama HTTP API with strict JSON enforcement.

        Args:
            messages: List of message dictionaries.

        Returns:
            Parsed JSON dictionary from response.

        Raises:
            RuntimeError: If Ollama connection fails or is not running.
            ValueError: If response does not contain valid JSON.
        """
        url = "http://localhost:11434/api/chat"
        
        # Strict JSON mode parameters
        params = {
            "model": self.ollama_model,
            "messages": messages,
            "options": {
                "temperature": 0.1,  # Low temperature for deterministic output
                "num_ctx": 8192,  # Context window
                "top_p": 0.9,  # Nucleus sampling
                "repeat_penalty": 1.1,  # Reduce repetition
                "mirostat": 0,  # Disable mirostat
                "num_predict": 2048,  # Max tokens
            },
            "format": "json",  # HARD JSON MODE - force JSON output
            "stream": False  # Disable streaming to get complete response
        }
        
        # Retry once if response doesn't start with {
        for attempt in range(2):
            try:
                response = self._requests.post(url, json=params, timeout=300)
                response.raise_for_status()
                
                # Parse response
                try:
                    result = response.json()
                except ValueError:
                    # If JSON parsing fails, try to extract from text
                    text = response.text.strip()
                    # Guard: reject if doesn't start with {
                    if not text.startswith("{"):
                        if attempt == 0:
                            continue  # Retry once
                        raise ValueError(f"Response does not start with brace: {text[:200]}")
                    return self._extract_json(text)
                
                # Ollama returns message content
                if "message" in result and "content" in result["message"]:
                    content = result["message"]["content"].strip()
                elif "response" in result:
                    content = result["response"].strip()
                else:
                    # If format is JSON and we get the content directly
                    if self.json_mode and isinstance(result, dict):
                        # Check if result itself is the JSON we want
                        if "type" not in result and "message" not in result:
                            return result
                    raise ValueError(f"Unexpected Ollama response format: {result}")
                
                # Guard: reject if doesn't start with {
                if not content.startswith("{"):
                    if attempt == 0:
                        continue  # Retry once with same prompt
                    raise ValueError(f"Response does not start with brace: {content[:200]}")
                
                return self._extract_json(content)
                
            except self._requests.exceptions.ConnectionError:
                raise RuntimeError(
                    f"Could not connect to Ollama at {url}. "
                    "Please make sure Ollama is installed and running.\n"
                    "Install from: https://ollama.ai\n"
                    f"Then run: ollama pull {self.ollama_model}"
                )
            except Exception as e:
                error_msg = str(e)
                if "Connection" in error_msg or "refused" in error_msg.lower():
                    raise RuntimeError(
                        f"Ollama is not running. Please start Ollama and ensure the model is available.\n"
                        f"Install Ollama from: https://ollama.ai\n"
                        f"Then run: ollama pull {self.ollama_model}"
                    )
                if attempt == 1:  # Last attempt
                    raise RuntimeError(f"Ollama API error after retry: {error_msg}")
                # Continue to retry on first attempt
    
    def _extract_json(self, text: str) -> Dict[str, Any]:
        """Extract JSON from text response.

        Handles markdown code fences and extracts only the top-level JSON object.

        Args:
            text: Text that may contain JSON, possibly wrapped in markdown.

        Returns:
            Parsed JSON dictionary (top-level object only).

        Raises:
            ValueError: If no valid JSON object found or parsing fails.
        """
        t = text.strip()
        
        # Strip code fences if present
        if t.startswith("```"):
            t = t.strip("`")
            # Remove language tag if present
            ln = t.find("\n")
            t = t[ln+1:] if ln != -1 else t
        
        # Find the largest top-level JSON object
        start, depth, end = -1, 0, -1
        for i, ch in enumerate(t):
            if ch == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        
        if start == -1 or end == -1:
            raise ValueError(f"No top-level JSON object found in response: {t[:200]}")
        
        json_str = t[start:end+1]
        
        try:
            parsed = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON: {str(e)}\nText: {json_str[:200]}")
        
        if not isinstance(parsed, dict):
            raise ValueError("Top-level JSON must be an object")
        
        return parsed

