import string
import os

class Decryptor:
    # Path to the English words dictionary in utils directory
    ENGLISH_WORDS_PATH = os.path.join('src', 'utils', 'english_words.txt')

    def __init__(self):
        """Initialize the Decryptor."""
        pass

    def decrypt(self, text: str, words_file: str = None) -> tuple[str, int]:
        """
        Attempt to decrypt text by trying all possible keys and comparing with english_words.txt.
        
        Args:
            text: Text to decrypt
            words_file: Optional path to file containing English words. If not provided, uses default ENGLISH_WORDS_PATH
            
        Returns:
            Tuple of (decrypted text, key used)
        """
        # Use provided words file or default path
        words_file = words_file or self.ENGLISH_WORDS_PATH
        
        # Load English words
        with open(words_file, 'r') as f:
            english_words = set(word.strip().lower() for word in f)
        
        best_score = 0
        best_key = 0
        best_text = text
        
        # Try each possible key
        for key in range(1, 26):
            # Decrypt with current key
            decrypted = self._caesar_cipher(text, key)
            
            # Count valid English words
            words = decrypted.lower().split()
            score = sum(1 for word in words if word.strip(string.punctuation) in english_words)
            
            # Update best result if this key has more valid words
            if score > best_score:
                best_score = score
                best_key = key
                best_text = decrypted
        
        return best_text, best_key

    def _caesar_cipher(self, text: str, key: int) -> str:
        """
        Apply Caesar cipher to text.
        
        Args:
            text: Text to encrypt/decrypt
            key: Shift key (1-25)
            
        Returns:
            Encrypted/decrypted text
        """
        result = []
        for char in text:
            if char.isalpha():
                # Determine the case
                ascii_offset = ord('a') if char.islower() else ord('A')
                # Apply the shift (subtract key for decryption)
                shifted = (ord(char) - ascii_offset - key) % 26
                result.append(chr(shifted + ascii_offset))
            else:
                # Keep non-alphabetic characters unchanged
                result.append(char)
        return ''.join(result) 