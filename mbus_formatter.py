import pandas as pd
import csv
import time

# Keep dictionary of sensor specific variables
# Prefixes are determined by VIF in M-Bus package
# [location, last_min_pressure_VIF, last_max_pressure_VIF, last_inst_pressure_VIF
# [location, last_flow1_VIF, last_flow2_VIF, last_temp_VIF, last_flow1_calc, last_flow2_calc]
sensor_info_dict = {
    "770004242c2d": ["loc-1", "69", "69", "69"], # PressureSensor
    "688268302c2d": ["loc-1", "13", "13", "67", -1, -1], # flowIQ
    "50902542ce9a": ["loc-2", "69", "69", "69"], # Simulated PressureSensor
    "51705369ce9a": ["loc-2", "13", "13", "67", -1, -1], # Simulated flowIQ
    "51705518ce9a": ["loc-3", "69", "69", "69"], # Simulated PressureSensor
    "51705538ce9a": ["loc-3", "13", "13", "67", -1, -1], # Simulated flowIQ
    "50902294ce9a": ["loc-4", "69", "69", "69"], # Simulated PressureSensor
    "51705516ce9a": ["loc-4", "13", "13", "67", -1, -1], # Simulated flowIQ
}

def calculate_pressure(VIF, D1, D2):
    """
    Calculate pressure on M-bus format
    """
    # Extract bit 1 and 2 from hex number located at temp_pac[22]
    prefix_bin = bin(int(VIF, 16))[2:].zfill(8)[6:9]
    prefix = 10 ** (int(prefix_bin, 2) - 3)
    hex_value = D1 + D2
    dec_value = int(hex_value, 16) * prefix
    return round(dec_value, 2)

def calculate_volume(VIF, D1, D2, D3, D4):
    """
    Calculate volume on M-bus format
    """
    # Extract bit 1 and 2 from hex number located at temp_pac[23]
    prefix_bin = bin(int(VIF, 16))[2:].zfill(8)[5:9]
    prefix = 10 ** (int(prefix_bin, 2) - 6)
    hex_value = D1 + D2 + D3 + D4
    int_value = int(hex_value, 16) * prefix
    return round(int_value, 3)

def calculate_temperature(VIF, D1):
    """
    Calculate temperature on M-bus format
    """
    # Extract bit 1 and 2 from hex number located at temp_pac[22]
    prefix_bin = bin(int(VIF, 16))[2:].zfill(8)[6:9]
    prefix = 10 ** (int(prefix_bin, 2) - 3)
    hex_value = D1
    #print("variables are ", str(hex_value), " and ", str(prefix))
    int_value = int(hex_value, 16) * prefix
    #print("returning ", str(int_value))
    return int(int_value)

def calculate_pressure_packet(pac_list, i1, i2, i3):
    device_name = pac_list[8] + pac_list[7] + pac_list[6] + pac_list[5] + pac_list[4] + pac_list[3]

    # [location, last_min_pressure_VIF, last_max_pressure_VIF, last_inst_pressure_VIF
    global sensor_info_dict
    last_min_pressure_VIF = sensor_info_dict[device_name][1]
    last_max_pressure_VIF = sensor_info_dict[device_name][2]
    last_inst_pressure_VIF = sensor_info_dict[device_name][3]

    # Min pressure
    press_min_calc = calculate_pressure(
        last_min_pressure_VIF, pac_list[i1], pac_list[i1 - 1]
    )
    pressure_pac = ";;;;;;" + str(press_min_calc)

    # Max pressure
    press_max_calc = calculate_pressure(
        last_max_pressure_VIF, pac_list[i2], pac_list[i2 - 1]
    )
    pressure_pac += ";" + str(press_max_calc)

    # Instant pressure
    press_inst_calc = calculate_pressure(
        last_inst_pressure_VIF, pac_list[i3], pac_list[i3 - 1]
    )
    pressure_pac += ";" + str(press_inst_calc)
    return pressure_pac

def calculate_flow_packet(pac_list, i1, i2, i3):
    device_name = pac_list[8] + pac_list[7] + pac_list[6] + pac_list[5] + pac_list[4] + pac_list[3]
    # [location, last_flow1_VIF, last_flow2_VIF, last_temp_VIF, last_flow1_calc, last_flow2_calc]
    global sensor_info_dict
    last_flow1_VIF = sensor_info_dict[device_name][1]
    last_flow2_VIF = sensor_info_dict[device_name][2]
    last_temp_VIF = sensor_info_dict[device_name][3]
    last_flow1_calc = sensor_info_dict[device_name][4]
    last_flow2_calc = sensor_info_dict[device_name][5]
    # flow 1
    volume1_calc = calculate_volume(
        last_flow1_VIF,
        pac_list[i1],
        pac_list[i1 - 1],
        pac_list[i1 - 2],
        pac_list[i1 - 3],
    )
    flow_pac = ";" + str(volume1_calc)

    # flow 2
    volume2_calc = calculate_volume(
        last_flow2_VIF,
        pac_list[i2],
        pac_list[i2 - 1],
        pac_list[i2 - 2],
        pac_list[i2 - 3],
    )
    flow_pac += ";" + str(volume2_calc)

    # temperature
    #print("lets calculate som temperature with ", str(i3))
    temp_calc = calculate_temperature(last_temp_VIF, pac_list[i3])
    flow_pac += ";" + str(temp_calc)

    # diff 1 & 2
    if last_flow1_calc == -1:
        flow_pac += ";" + "0"
        flow_pac += ";" + "0"
        sensor_info_dict[device_name][4] = volume1_calc
        sensor_info_dict[device_name][5] = volume2_calc
    else:
        flow1_diff = int(1000 * volume1_calc) - int(1000 * last_flow1_calc)
        flow2_diff = int(1000 * volume2_calc) - int(1000 * last_flow2_calc)
        flow_pac += ";" + str(flow1_diff)
        flow_pac += ";" + str(flow2_diff)
        sensor_info_dict[device_name][4] = volume1_calc
        sensor_info_dict[device_name][5] = volume2_calc
    flow_pac += ";;;"
    return flow_pac

def format_packet(pac):
    """
    Format packets received on M-bus format into data that is readable
    """
    global sensor_info_dict

    temp_pac = pac.split(";")
    device_name = temp_pac[8] + temp_pac[7] + temp_pac[6] + temp_pac[5] + temp_pac[4] + temp_pac[3]

    new_pac = ""

    # Time package was received
    new_pac += time.strftime("%H:%M:%S", time.gmtime(int(temp_pac[0])))

    packet_type = temp_pac[10]
    man_id_1 = temp_pac[3]
    man_id_2 = temp_pac[4]

    if man_id_1 == "2d" and man_id_2 == "2c" and packet_type == "16": # Kamstrup flowIQ
        if temp_pac[20] == "78": # VIF is transmitted
            # last_flow1_VIF
            sensor_info_dict[device_name][1] = temp_pac[27]
            # last_flow2_VIF
            sensor_info_dict[device_name][2] = temp_pac[33]
            # last_temp_VIF
            sensor_info_dict[device_name][3] = temp_pac[39]
            i1 = 31
            i2 = 37
            i3 = 40
        elif temp_pac[20] == "79": # VIF is not transmitted
            i1 = 30
            i2 = 34
            i3 = 35
        new_pac += calculate_flow_packet(temp_pac, i1, i2, i3)
    
    elif man_id_1 == "2d" and man_id_2 == "2c" and packet_type == "18": # Kamstrup PressureSensor
        if temp_pac[20] == "78": # VIF is transmitted
            # last_min_pressure_VIF
            sensor_info_dict[device_name][1] = temp_pac[22]
            # last_max_pressure_VIF
            sensor_info_dict[device_name][2] = temp_pac[26]
            # last_inst_pressure_VIF
            sensor_info_dict[device_name][3] = temp_pac[30]
            i1 = 24
            i2 = 28
            i3 = 32
        elif temp_pac[20] == "79": # VIF is not transmitted
            i1 = 26
            i2 = 28
            i3 = 30
        new_pac += calculate_pressure_packet(temp_pac, i1, i2, i3)

    elif man_id_1 == "9a" and man_id_2 == "ce" and packet_type == "16": # Simulated flowIQ
        i1 = 31
        i2 = 35
        i3 = 36
        new_pac += calculate_flow_packet(temp_pac, i1, i2, i3)

    elif man_id_1 == "9a" and man_id_2 == "ce" and packet_type == "18": # Simulated PressureSensor
        i1 = 27
        i2 = 29
        i3 = 31
        new_pac += calculate_pressure_packet(temp_pac, i1, i2, i3)

    else:
        print("unknown packet")

    rssi_hex = str(pac[-2]) + str(pac[-1])
    rssi_int = int(rssi_hex, 16)
    new_pac += ";" + str(rssi_int)

    return new_pac

def print_packet(packet):
    """
    Print a data packet
    """
    packet_list = packet.split(";")
    for i in packet_list:
        print(i, end="\t")
    print(" ")

def save_packet(save_loc, packet):
    """
    Save a data packet in a csv format
    """
    with open(save_loc, "a", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow([packet])

def main():
    source_location = "../../data/2021/05-mai/"
    flow_meter = "688268302c2d"
    pressure_meter = "770004242c2d"
    date = "2021-05-22"

    flow_file = source_location + flow_meter + "-" + date + '.csv'
    pressure_file = source_location + pressure_meter + "-" + date + '.csv'

    formatted_save_loc = source_location + sensor_info_dict[flow_meter][0] + "_" + date + "-formatted.csv"

    with open(flow_file) as csv_flow_file:
        csv_reader = csv.reader(csv_flow_file)
        for row in csv_reader:
            #print(row)
            formatted_packet = format_packet(row[0])
            print_packet(flow_meter + ";" + formatted_packet)
            save_packet(formatted_save_loc, formatted_packet)

    with open(pressure_file) as csv_pressure_file:
        csv_reader = csv.reader(csv_pressure_file)
        for row in csv_reader:
            #print(row)
            formatted_packet = format_packet(row[0])
            print_packet(pressure_meter + ";" + formatted_packet)
            save_packet(formatted_save_loc, formatted_packet)


if __name__ == "__main__":
    main()
