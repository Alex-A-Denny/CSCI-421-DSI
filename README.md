# CSCI-421-DSI

Group 5 Database System Implementation

Denny, Alex, aad7700\
Ellis, Sam, sae1200\
Tokumoto, Brian, bht8183\
Warren, Spencer, sbw7538\
Zhou, Beining, bz5529

## Building Instructions

To run: java Main \<db loc\> \<page size\> \<buffer size\>
   <!-- java Main <db loc> <page size> <buffer size> -->

## Project Structure

The Catalog of our database stores information about table schema and parameters.
(Table ID, Table Name, List of record layouts, List of pages for each table, 
and the given Page Size). It also assigns a new, unique ID when a page or table is added.

