import random
import string
from typing import Tuple, List
import json
import os

class Encryptor:
    """Handles encryption and decryption of sensitive data using Caesar cipher."""
    
    def __init__(self, state_store_path: str = 'state/encryption_keys.json'):
        self.state_store_path = state_store_path
        os.makedirs(os.path.dirname(state_store_path), exist_ok=True)
        self._load_state()
        
    def _load_state(self):
        """Load encryption state from file."""
        if os.path.exists(self.state_store_path):
            with open(self.state_store_path, 'r') as f:
                self.state = json.load(f)
        else:
            self.state = {}
            
    def _save_state(self):
        """Save encryption state to file."""
        with open(self.state_store_path, 'w') as f:
            json.dump(self.state, f)
            
    def encrypt(self, text: str, file_name: str) -> Tuple[str, int]:
        """
        Encrypt text using Caesar cipher with a random key.
        Stores the key in state store for future decryption.
        """
        key = random.randint(1, 25)
        encrypted_text = self._caesar_cipher(text, key)
        
        # Store the key
        self.state[file_name] = key
        self._save_state()
        
        return encrypted_text, key
        
    def decrypt(self, text: str, file_name: str = None, key: int = None) -> str:
        """
        Decrypt text using Caesar cipher.
        If file_name is provided, uses key from state store.
        If key is provided directly, uses that instead.
        """
        if key is None and file_name in self.state:
            key = self.state[file_name]
        elif key is None:
            raise ValueError("No encryption key found for file")
            
        return self._caesar_cipher(text, -key)  # Negative key for decryption
        
    def _caesar_cipher(self, text: str, shift: int) -> str:
        """Implementation of Caesar cipher algorithm."""
        result = ""
        for char in text:
            if char.isalpha():
                ascii_offset = ord('A') if char.isupper() else ord('a')
                shifted = (ord(char) - ascii_offset + shift) % 26
                result += chr(shifted + ascii_offset)
            else:
                result += char
        return result
        
    def keyless_decrypt(self, text: str, valid_words_file: str) -> str:
        """
        Attempt to decrypt text without a key by trying all possible shifts
        and selecting the one that produces the most valid English words.
        """
        # Load valid words
        with open(valid_words_file, 'r') as f:
            valid_words = set(word.strip().lower() for word in f)
            
        best_score = 0
        best_decryption = ""
        
        # Try all possible shifts
        for key in range(1, 26):
            decrypted = self._caesar_cipher(text, -key)
            words = decrypted.lower().split()
            score = sum(1 for word in words if word in valid_words)
            
            if score > best_score:
                best_score = score
                best_decryption = decrypted
                
        return best_decryption 