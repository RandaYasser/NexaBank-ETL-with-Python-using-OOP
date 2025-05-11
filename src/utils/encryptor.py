import random
import string

class Encryptor:
    def __init__(self):
        """ Initialize the Encryptor."""
        pass

    def encrypt(self, text: str, file_name: str) -> tuple[str, int]:
        """
        Encrypt text using a random key.
        
        Args:
            text: Text to encrypt
            file_name: Name of the file being encrypted
            
        Returns:
            Tuple of (encrypted text, key used)
        """
        # Generate a random key between 1 and 25
        key = random.randint(1, 25)
        
        # Encrypt the text
        encrypted_text = self._caesar_cipher(text, key)
        
        return encrypted_text, key

    def _caesar_cipher(self, text: str, key: int) -> str:
        """
        Apply the Caesar cipher to the text.
        
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
                # Apply the shift
                shifted = (ord(char) - ascii_offset + key) % 26
                result.append(chr(shifted + ascii_offset))
            else:
                # Keep non-alphabetic characters unchanged
                result.append(char)
        return ''.join(result) 
