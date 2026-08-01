AA:
 - AA shaded by attributeNo%totalShards
 - Use blocking in the AA like datatype, LSH or min hashing, incoming to minhash and then compare

Protocol for RA, AA, CM synchronization with buffering state:
    AA reports possible pairs to RA, RA acknowledges
    RA asks violation histories with witness from VO/ATTR, now VO/ATTR starts buffering these updates, acknowledges RA 
    VO/ATTR sends the violation histories to CM, CM acknowledges and request buffered and other updates to VO/ATTR
    VO/ATTR sends the buffered data and other updates to CM, CM acknowledges and handles
    CM finds too many violations, mark the pairs not active and stops tracking, updates the VO/ATTR for no updates and
    also send to RA for inactiveness.
    

Use of Epoch or timestamp for rebuilding

Use of Sliding window for RA on VO/ATTR

Look into design of only storing the change / updated values for VO/ATTR

RA test for similarities for inactive pairs only but not for active pairs.

Back pressure and pull architecture for synchronization



