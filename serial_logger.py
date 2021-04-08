"""
Required packages:
python3
Microsoft C++ Build Tools (Microsoft Visual C++ 14.0)
pip install requests pyserial python_jwt sseclient pycryptodome requests-toolbelt AWSIoTPythonSDK
* Might have to change crypto to Crypto in AppData\Local\Programs\Python\Python39\Lib\site-packages
"""

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import serial
import serial.tools.list_ports as list_ports
import csv
from datetime import datetime
import time
import json

c_dict = {
  "44": "SND-NR"
}

type_dict = {
  "07": "Water",
  "16": "Cold Water",
  "18": "Pressure"
}

# Prefixes are determined by VIF in M-Bus package
last_min_pressure_VIF = "69"
last_max_pressure_VIF = "69"
last_inst_pressure_VIF = "69"
last_flow1_VIF = "13"
last_flow2_VIF = "13"
last_temp_VIF = "67"
last_flow1_calc = -1
last_flow2_calc = -1

# Constants for AWS cloud upload
"""
clientId = "mbus-collector"
host = "a2ap02hejjfikb-ats.iot.eu-north-1.amazonaws.com" 
cloud_port = 8883
rootCAPath = "root-CA.crt"
privateKeyPath = "mbus-collector.private.key"
certificatePath = "mbus-collector.cert.pem"
"""

def main():
    #print_ports()
    log_port("COM10")

###############################################################################

def print_ports():
    ports = list(list_ports.comports())
    for p in ports:
        print(p)

def calculate_pressure(VIF, D1, D2):
    # Extract bit 1 and 2 from hex number located at temp_pac[22]
    prefix_bin = bin(int(VIF, 16))[2:].zfill(8)[6:9]
    prefix = 10**(int(prefix_bin, 2) - 3)
    hex_value = D1 + D2
    int_value = int(hex_value, 16) * prefix
    return int_value

def calculate_volume(VIF, D1, D2, D3, D4):
    # Extract bit 1 and 2 from hex number located at temp_pac[23]
    prefix_bin = bin(int(VIF, 16))[2:].zfill(8)[5:9]
    prefix = 10**(int(prefix_bin, 2) - 6)
    hex_value = D1 + D2 + D3 + D4
    int_value = int(hex_value, 16) * prefix
    return int_value

def calculate_temperature(VIF, D1):
    # Extract bit 1 and 2 from hex number located at temp_pac[22]
    prefix_bin = bin(int(VIF, 16))[2:].zfill(8)[6:9]
    prefix = 10**(int(prefix_bin, 2) - 3)
    hex_value = D1
    int_value = int(hex_value, 16) * prefix
    return int_value

def format_packet(pac):
    global last_min_pressure_VIF
    global last_max_pressure_VIF
    global last_inst_pressure_VIF
    global last_flow1_VIF
    global last_flow2_VIF
    global last_temp_VIF
    global last_flow1_calc
    global last_flow2_calc

    temp_pac = pac.split(';')
    new_pac = ""
    # Time package was received
    new_pac += time.strftime('%H:%M:%S', time.gmtime(int(temp_pac[0])))
    # Length byte
    # new_pac += str(int(temp_pac[1], 16)) + ";"
    # C byte
    # new_pac += c_dict[temp_pac[2]] + ";"
    # T byte
    # new_pac += type_dict[temp_pac[10]] + ";"
    if temp_pac[10] == "16": # flow
        if temp_pac[20] == "78": # If data for VIF is transmitted
            last_flow1_VIF = temp_pac[27]
            last_flow2_VIF = temp_pac[33]
            last_temp_VIF = temp_pac[39]
            i1 = 31
            i2 = 37
            i3 = 40
        elif temp_pac[11] == "7a": # Silabs packet
            i1 = 31
            i2 = 35
            i3 = 36
        else: # It's a concentrated kamstrup packet
            i1 = 30
            i2 = 34
            i3 = 35

        # flow 1
        volume1_calc = calculate_volume(last_flow1_VIF, temp_pac[i1], temp_pac[i1-1], temp_pac[i1-2], temp_pac[i1-3])
        new_pac += ";" + str(volume1_calc)
        # flow 2
        volume2_calc = calculate_volume(last_flow2_VIF, temp_pac[i2], temp_pac[i2-1], temp_pac[i2-2], temp_pac[i2-3])
        new_pac += ";" + str(volume2_calc)
        # temperature
        temp_calc = calculate_temperature(last_temp_VIF, temp_pac[i3])
        new_pac += ";" + str(temp_calc)
        # diff 1 & 2
        if last_flow1_calc == -1:
            new_pac += ";;"
            last_flow1_calc = volume1_calc
            last_flow2_calc = volume2_calc
        else:
            flow1_diff = 1000 * (volume1_calc - last_flow1_calc)
            flow2_diff = 1000 * (volume2_calc - last_flow2_calc)
            new_pac += ";" + str(flow1_diff)
            new_pac += ";" + str(flow2_diff)
            last_flow1_calc = volume1_calc
            last_flow2_calc = volume2_calc

        rssi_hex = str(pac[-2]) + str(pac[-1])
        rssi_int = int(rssi_hex, 16)
        new_pac += ";;;;" + str(rssi_int)
    
    elif temp_pac[10] == "18": # pressure
        if temp_pac[20] == "78": # If data for VIF is transmitted
            last_min_pressure_VIF = temp_pac[22]
            last_max_pressure_VIF = temp_pac[26]
            last_inst_pressure_VIF = temp_pac[30]
            i1 = 24
            i2 = 28
            i3 = 32
        elif temp_pac[11] == "7a": # Silabs packet
            i1 = 26
            i2 = 28
            i3 = 30
        else: # It's a concentrated kamstrup packet
            i1 = 26
            i2 = 28
            i3 = 30

        # Min pressure
        press_min_calc = calculate_pressure(last_min_pressure_VIF, temp_pac[i1], temp_pac[i1-1])
        new_pac += ";;;;;;" + str(press_min_calc)
        # Max pressure
        press_max_calc = calculate_pressure(last_max_pressure_VIF, temp_pac[i2], temp_pac[i2-1])
        new_pac += ";" + str(press_max_calc)
        # Instant pressure
        press_inst_calc = calculate_pressure(last_inst_pressure_VIF, temp_pac[i3], temp_pac[i3-1])
        new_pac += ";" + str(press_inst_calc)

        rssi_hex = str(pac[-2]) + str(pac[-1])
        rssi_int = int(rssi_hex, 16)
        new_pac += ";" + str(rssi_int)


    return new_pac


""" Read serial port and save the data to file and cloud """
def log_port(port):
    """
    myAWSIoTMQTTClient = None
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
    myAWSIoTMQTTClient.configureEndpoint(host, cloud_port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

    # AWSIoTMQTTClient connection configuration
    myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
    myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
    myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
    myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
    myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

    # Connect to AWS IoT
    myAWSIoTMQTTClient.connect()
    """
    ser = serial.Serial(port, 19200)  # open serial port.
    print(ser.name)
    ser.reset_input_buffer() # Discard all content of input buffer

    while True:
        try:
            device_name = ""
            ser_byte = ser.read() # Read first byte
            # Convert to hex
            in_hex = ser_byte.hex()
            packet = in_hex
            
            # Time and date calculation
            date_today = datetime.today().strftime("%Y-%m-%d")
            now = datetime.now()
            seconds_since_midnight = int((now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds())

            ###################################################################
            # Read all packets
            ###################################################################
            for i in range(int(in_hex, 16)):
                ser_byte = ser.read()
                in_hex = ser_byte.hex()
                packet += ";" + in_hex

                if i <= 6 and i > 0: # Store all bytes for device name
                    device_name = in_hex + device_name

            full_packet = str(seconds_since_midnight) + ";" + packet

            #print(full_packet)

            ###################################################################
            # Save raw data to file
            ###################################################################
            """
            save_loc = device_name + "-" + date_today + ".csv"
            with open(save_loc,"a",newline="") as f:
                writer = csv.writer(f, delimiter=",")
                writer.writerow([full_packet])
            """

            ###################################################################
            # Format data
            ###################################################################
            formatted_packet = format_packet(full_packet)
            print(formatted_packet)

            ###################################################################
            # Save formatted data to file
            ###################################################################
            """
            formatted_save_loc = date_today + "-formatted.csv"
            with open(formatted_save_loc,"a",newline="") as f:
                writer = csv.writer(f, delimiter=",")
                writer.writerow([formatted_packet])

            formatted_packet_list = formatted_packet.split(';')
            full_packet_list = full_packet.split(';')

            for i in formatted_packet_list:
                print(i, end="\t")
            print(" ")
            """
            ###################################################################
            # Cloud upload
            ###################################################################
            """
            # Calculate topic name            
            sensor_type = "Unknown"
            data_to_upload = {}
            if full_packet_list[10] == "16":
                sensor_type = "flow"
                data_to_upload = {
                    'Time' : formatted_packet_list[0],
                    'flow_max_month' : formatted_packet_list[1],
                    'flow_inst' : formatted_packet_list[2],
                    'temp_ambient' : formatted_packet_list[3],
                    'flow_max_month_diff' : formatted_packet_list[4],
                    'flow_inst_diff' : formatted_packet_list[5],
                    'RSSI' : formatted_packet_list[9]
                }
            elif full_packet_list[10] == "18":
                sensor_type = "pressure"
                data_to_upload = {
                    'Time' : formatted_packet_list[0],
                    'min_pressure' : formatted_packet_list[6],
                    'max_pressure' : formatted_packet_list[7],
                    'inst_pressure' : formatted_packet_list[8],
                    'RSSI' : formatted_packet_list[9]
                }

            topic = "collector1" + "/" + "location1" + "/" + sensor_type + device_name + "/" + date_today
            messageJson = json.dumps(data_to_upload)
            try:
                myAWSIoTMQTTClient.publish(topic, messageJson, 1)
                #print('Published topic %s: %s\n' % (topic, messageJson))
            except Exception:
                print("Timeout")
            """

        except Exception:
            error_time = datetime.today().strftime("%Y-%m-%d/%H:%M:%S")
            print("Error occured at", error_time)
            with open("error_log.csv","a",newline="") as f:
                writer = csv.writer(f, delimiter=",")
                writer.writerow([error_time])
            print("Continuing logging...")
            continue

if __name__ == "__main__":
    main()
