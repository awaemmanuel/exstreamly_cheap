'''
    Module creates a hash to be used as key
    for a key/value store.
    
    Author: Emmanuel Awa
'''
import hashlib

def _hashed_string(string):
    ''' Create SHA256 Hash of string '''
    return hashlib.sha256(string.encode())

def create_hashedkey_and_stringval(string):
    ''' Create a dictionary of hashed key and string as val '''
    return {_hashed_string(string).hexdigest(): string}
    
    
    