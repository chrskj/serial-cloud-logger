"""
To install all required libraries:
$ pip install -r requirements.txt
"""

import click
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import serial
import serial.tools.list_ports as list_ports
import csv
from datetime import datetime
import time
import json

__author__ = "Christofer Gilje Skjaeveland"

###############################################################################
# Global variables
###############################################################################

# Constants for AWS cloud upload
clientId = "mbus-collector-2"
host = "a2ap02hejjfikb-ats.iot.eu-west-1.amazonaws.com"
cloud_port = 8883
rootCAPath = "root-CA.crt"
privateKeyPath = "mbus-collector.private.key"
certificatePath = "mbus-collector.cert.pem"

# Keep dictionary of sensor specific variables
# Prefixes are determined by VIF in M-Bus package
# [location, last_min_pressure_VIF, last_max_pressure_VIF, last_inst_pressure_VIF
# [location, last_flow1_VIF, last_flow2_VIF, last_temp_VIF, last_flow1_calc, last_flow2_calc]
sensor_info_dict = {
    "770004242c2d": ["loc-1", "69", "69", "69"],  # PressureSensor
    "688268302c2d": ["loc-1", "13", "13", "67", -1, -1],  # flowIQ
    "50902542ce9a": ["loc-2", "69", "69", "69"],  # Simulated PressureSensor
    "51705369ce9a": ["loc-2", "13", "13", "67", -1, -1],  # Simulated flowIQ
    "51705518ce9a": ["loc-3", "69", "69", "69"],  # Simulated PressureSensor
    "51705538ce9a": ["loc-3", "13", "13", "67", -1, -1],  # Simulated flowIQ
    "50902294ce9a": ["loc-4", "69", "69", "69"],  # Simulated PressureSensor
    "51705516ce9a": ["loc-4", "13", "13", "67", -1, -1],  # Simulated flowIQ
}


###############################################################################
# Main function
###############################################################################
@click.group()
def main():
    """
    Script for logging wM-Bus packets on a port
    """
    pass


###############################################################################
# Functions
###############################################################################


@main.command()
def print_ports():
    """
    Print available data ports
    """
    ports = list(list_ports.comports())
    for p in ports:
        click.echo(p)


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
    # print("variables are ", str(hex_value), " and ", str(prefix))
    int_value = int(hex_value, 16) * prefix
    # print("returning ", str(int_value))
    return int(int_value)


def calculate_pressure_packet(pac_list, i1, i2, i3):
    device_name = (
        pac_list[8]
        + pac_list[7]
        + pac_list[6]
        + pac_list[5]
        + pac_list[4]
        + pac_list[3]
    )

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
    device_name = (
        pac_list[8]
        + pac_list[7]
        + pac_list[6]
        + pac_list[5]
        + pac_list[4]
        + pac_list[3]
    )
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
    # print("lets calculate som temperature with ", str(i3))
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
    device_name = (
        temp_pac[8]
        + temp_pac[7]
        + temp_pac[6]
        + temp_pac[5]
        + temp_pac[4]
        + temp_pac[3]
    )

    new_pac = ""

    # Time package was received
    new_pac += time.strftime("%H:%M:%S", time.gmtime(int(temp_pac[0])))

    packet_type = temp_pac[10]
    man_id_1 = temp_pac[3]
    man_id_2 = temp_pac[4]

    if (
        man_id_1 == "2d" and man_id_2 == "2c" and packet_type == "16"
    ):  # Kamstrup flowIQ
        if temp_pac[20] == "78":  # VIF is transmitted
            # last_flow1_VIF
            sensor_info_dict[device_name][1] = temp_pac[27]
            # last_flow2_VIF
            sensor_info_dict[device_name][2] = temp_pac[33]
            # last_temp_VIF
            sensor_info_dict[device_name][3] = temp_pac[39]
            i1 = 31
            i2 = 37
            i3 = 40
        elif temp_pac[20] == "79":  # VIF is not transmitted
            i1 = 30
            i2 = 34
            i3 = 35
        new_pac += calculate_flow_packet(temp_pac, i1, i2, i3)

    elif (
        man_id_1 == "2d" and man_id_2 == "2c" and packet_type == "18"
    ):  # Kamstrup PressureSensor
        if temp_pac[20] == "78":  # VIF is transmitted
            # last_min_pressure_VIF
            sensor_info_dict[device_name][1] = temp_pac[22]
            # last_max_pressure_VIF
            sensor_info_dict[device_name][2] = temp_pac[26]
            # last_inst_pressure_VIF
            sensor_info_dict[device_name][3] = temp_pac[30]
            i1 = 24
            i2 = 28
            i3 = 32
        elif temp_pac[20] == "79":  # VIF is not transmitted
            i1 = 26
            i2 = 28
            i3 = 30
        new_pac += calculate_pressure_packet(temp_pac, i1, i2, i3)

    elif (
        man_id_1 == "9a" and man_id_2 == "ce" and packet_type == "16"
    ):  # Simulated flowIQ
        i1 = 31
        i2 = 35
        i3 = 36
        new_pac += calculate_flow_packet(temp_pac, i1, i2, i3)

    elif (
        man_id_1 == "9a" and man_id_2 == "ce" and packet_type == "18"
    ):  # Simulated PressureSensor
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


def init_aws_upload(myAWSIoTMQTTClient):
    """
    Initialize AWS uploading
    """
    myAWSIoTMQTTClient.configureEndpoint(host, cloud_port)
    myAWSIoTMQTTClient.configureCredentials(
        rootCAPath, privateKeyPath, certificatePath
    )

    # AWSIoTMQTTClient connection configuration
    myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
    myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Set as infinite
    myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
    myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
    myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

    # Connect to AWS IoT
    myAWSIoTMQTTClient.connect()


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


@main.command()
@click.argument("port")
@click.option("-pr", "--print-raw-packets", type=bool, default=True)
@click.option("-sr", "--save-raw-packets", type=bool, default=True)
@click.option("-f", "--format-packets", type=bool, default=False)
@click.option("-pf", "--print-formatted-packets", type=bool, default=False)
@click.option("-sf", "--save-formatted-packets", type=bool, default=False)
@click.option("-u", "--upload-packets", type=bool, default=False)
def log_port(
    port,
    print_raw_packets,
    save_raw_packets,
    format_packets,
    print_formatted_packets,
    save_formatted_packets,
    upload_packets,
):
    """
    Read serial port and choose between several options
    """
    if upload_packets or save_formatted_packets or print_formatted_packets:
        format_packets = True

    myAWSIoTMQTTClient = None
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
    init_aws_upload(myAWSIoTMQTTClient)

    ser = serial.Serial(port, 19200)  # open serial port.
    print(ser.name)
    ser.reset_input_buffer()  # Discard all content of input buffer

    while True:
        try:
            ###################################################################
            # Read all packets
            ###################################################################
            device_name = ""
            ser_byte = ser.read()  # Read first byte to determine length
            in_hex = ser_byte.hex()  # Convert to hex
            packet = in_hex

            # Read the rest of the bytes
            for i in range(int(in_hex, 16)):
                ser_byte = ser.read()
                in_hex = ser_byte.hex()
                packet += ";" + in_hex

                if i <= 6 and i > 0:  # Store all bytes for device name
                    device_name = in_hex + device_name

            # Time and date calculation
            date_today = datetime.today().strftime("%Y-%m-%d")
            now = datetime.now()
            seconds_since_midnight = int(
                (
                    now - now.replace(hour=0, minute=0, second=0, microsecond=0)
                ).total_seconds()
            )

            timed_packet = str(seconds_since_midnight) + ";" + packet

            ###################################################################
            # Print raw Packets
            ###################################################################
            if print_raw_packets:
                print_packet(timed_packet)

            ###################################################################
            # Format packets
            ###################################################################
            if format_packets:
                formatted_packet = format_packet(timed_packet)

            ###################################################################
            # Print formatted packets
            ###################################################################
            if print_formatted_packets:
                print_packet(device_name + ";" + formatted_packet)

            ###################################################################
            # Save raw packets to file
            ###################################################################
            if save_raw_packets:
                raw_save_loc = device_name + "-" + date_today + ".csv"
                save_packet(raw_save_loc, timed_packet)

            ###################################################################
            # Save formatted packets to file
            ###################################################################
            if save_formatted_packets:
                formatted_save_loc = (
                    sensor_info_dict[device_name][0]
                    + "_"
                    + date_today
                    + "-formatted.csv"
                )
                save_packet(formatted_save_loc, formatted_packet)

            ###################################################################
            # Save formatted data to cloud
            ###################################################################
            if upload_packets:
                formatted_packet_list = formatted_packet.split(";")
                timed_packet_list = timed_packet.split(";")
                sensor_type = "Unknown"
                data_to_upload = {}

                # Define data to be uploaded
                if timed_packet_list[10] == "16":
                    sensor_type = "flow"
                    data_to_upload = {
                        # "Date": date_today + " " + formatted_packet_list[0],
                        "SerialNumber": device_name,
                        "CollectorID": clientId,
                        "Location": sensor_info_dict[device_name][0],
                        "flow_inst": float(formatted_packet_list[1]),
                        "flow_max_month": float(formatted_packet_list[2]),
                        "temp_ambient": int(formatted_packet_list[3]),
                        "flow_inst_diff": int(formatted_packet_list[4]),
                        "flow_max_month_diff": int(formatted_packet_list[5]),
                        "RSSI": int(formatted_packet_list[9]),
                    }
                elif timed_packet_list[10] == "18":
                    sensor_type = "pressure"
                    data_to_upload = {
                        # "Date": date_today + " " + formatted_packet_list[0],
                        "SerialNumber": device_name,
                        "CollectorID": clientId,
                        "Location": sensor_info_dict[device_name][0],
                        "min_pressure": float(formatted_packet_list[6]),
                        "max_pressure": float(formatted_packet_list[7]),
                        "inst_pressure": float(formatted_packet_list[8]),
                        "RSSI": int(formatted_packet_list[9]),
                    }

                # Define topic name
                topic = (
                    "collectors/"
                    + clientId
                    + "/"
                    + sensor_type
                    + "/"
                    + device_name
                )
                messageJson = json.dumps(data_to_upload)
                try:
                    myAWSIoTMQTTClient.publish(topic, messageJson, 1)
                    # print('Published topic %s: %s\n' % (topic, messageJson))
                except Exception as e:
                    print("Error: ", e)
            ###################################################################

        except Exception as e:
            error_time = datetime.today().strftime("%Y-%m-%d/%H:%M:%S")
            print("Error occured at", error_time)
            with open("error_log.csv", "a", newline="") as f:
                writer = csv.writer(f, delimiter=",")
                writer.writerow([error_time])
                writer.writerow([e])
            print("Continuing logging...")
            continue


if __name__ == "__main__":
    main()
