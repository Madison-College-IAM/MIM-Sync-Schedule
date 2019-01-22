# MIM-Sync-Schedule
Based on a script written by Kent Nordstrom (http://konab.com/scheduling-mim-advanced-options/). 

## Concept
The basic idea is to run imports and exports in parallel while running synchronizations in order.

## Additional Functionality
A function was added to create a delta table in SQL by comparing a copy of the source table to the current source table. 
