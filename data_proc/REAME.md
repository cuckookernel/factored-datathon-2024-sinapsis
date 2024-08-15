

## Quality issues found in data

Besides many nulls in many fields (see data exploration reports for details)

We found the following issues: 
    
  - `gkgcounts.NUMBER` (renamed to `reported_count`): 
     Causes integer overflow when reading (in a handful of rows), it is always the same value it seems:
    ```
    [15:34:32] INFO     a_str='081097300169928285810'  caused overflow, attempting cleanup                                                             utils.py:35
    ```
    Solution: Read as float and deal with unreasonably large values in a later stage.
