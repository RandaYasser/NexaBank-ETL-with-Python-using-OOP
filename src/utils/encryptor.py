import random
import string
import logging
class Encryptor:
    def __init__(self):
        """ Initialize the Encryptor."""

    @staticmethod
    def encrypt(text: str) -> str:
        """
        Encrypt text using a random key.
        
        Args:
            text: Text to encrypt
            
        Returns:
            encrypted text
        """
        # Generate a random key between 1 and 25
        key = random.randint(1, 25)
        
        # Encrypt the text
        encrypted_text = Encryptor._caesar_cipher(text, key)
        return encrypted_text

    @staticmethod
    def _caesar_cipher(text: str, key: int) -> str:
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
