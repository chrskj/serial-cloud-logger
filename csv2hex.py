"""
===============================================================================
Program to convert csv data to intel hex file format
===============================================================================
Hex File Format:

:llaaaatt[dd...]cc

   :    is the colon that starts every Intel HEX record.
   ll   is the record-length field that represents the number of data bytes (dd) 
        in the record.
   aaaa is the address field that represents the starting address for subsequent 
        data in the record.
   tt   is the field that represents the HEX record type, which may be one of 
        the following:
   00 - data record
   01 - end-of-file record
   02 - extended segment address record
   04 - extended linear address record
   05 - start linear address record (MDK-ARM only)
   dd   is a data field that represents one byte of data. A record may have
        multiple data bytes. The number of data bytes in the record must match
        the number specified by the ll field.
   cc   is the checksum field that represents the checksum of the record. The
        checksum is calculated by summing the values of all hexadecimal digit
        pairs in the record modulo 256 and taking the two's complement.
"""

from intelhex import IntelHex
import csv

hex_file = "test_data_hex.hex"
csv_file = "770004242c2d-2021-03-08_14.csv"

ih = IntelHex()

base_address = "0x8000"
base_address_int = int(base_address, 16)

with open(csv_file) as f:
    csv_reader = csv.reader(f, delimiter=";")
    line_count = 0
    el_count = 0
    for row in csv_reader:
        for el in row:
            el = int(el, 16)
            # print(el)
            ih[base_address_int + el_count] = el
            el_count += 1
        line_count += 1

print("Elements:", el_count)
print("lines:", line_count)
print("Base Address:", base_address)

ih.write_hex_file(hex_file)
