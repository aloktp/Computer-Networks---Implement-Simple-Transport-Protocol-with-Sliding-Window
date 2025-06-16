import socket
import sys
import threading
import time
from collections import OrderedDict

DATA = 0
ACK = 1
SYN = 2
FIN = 3
HEADER_SIZE = 4
MAX_PAYLOAD_SIZE = 1000

class Receiver:
    def __init__(self, receiver_port, sender_port, filename, max_window_size):
        self.receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.receiver_socket.bind(('localhost', receiver_port))
        self.sender_address = ('localhost', sender_port)
        self.filename = filename
        self.log_filename = f"receiver_log.txt"
        self.max_win = max_window_size/1000
        self.expected_seq_number = 0
        self.sliding_window = OrderedDict()
        self.connection_teardown_flag = 0
        self.amount_of_original_data_received = 0
        self.number_of_original_data_segments_received = 0
        self.number_of_duplicate_data_segments_received = 0
        self.number_of_duplicate_acknowledgments_sent = 0
        self.time_wait = threading.Timer(2, self.timewait_thread) 
        self.list_of_packet_seq_numbers_to_be_deleted_from_sliding_window = []

    def log_message(self, direction, time, type, ack_number, length): 
        with open(self.log_filename, 'a') as file:  
            file.write(f"{direction}    {round(time*1000,2)}    {type}  {ack_number}   {length}\n")

    def create_packet(self, type, seq_number): 
        """ Helper function to create a packet based on the type """
        header = type.to_bytes(2, byteorder='big') + seq_number.to_bytes(2, byteorder='big')
        return header

    def timewait_thread(self):
        while True:
            packet, _ = self.receiver_socket.recvfrom(MAX_PAYLOAD_SIZE + HEADER_SIZE)
            self.log_message("rcv", (time.time() - self.start_time), "ACK", int.from_bytes(packet[2:4]), len(packet[4:])) 
            packet_type = int.from_bytes(packet[:2], 'big')
            seq_number = int.from_bytes(packet[2:4], 'big')
            if packet_type == FIN: 
                self.expected_seq_number =  (seq_number + 1) % 65536
                ack_packet = self.create_packet(ACK, self.expected_seq_number)
                self.receiver_socket.sendto(ack_packet, self.sender_address)
                self.log_message("snd", (time.time() - self.start_time), "ACK", int.from_bytes(ack_packet[2:4]), len(ack_packet[4:])) 

    def handle_packets(self):
        count = 0 
        while self.connection_teardown_flag == 0:

            count = count + 1
            if(count == 1):
                self.start_time = time.time()
            packet, _ = self.receiver_socket.recvfrom(MAX_PAYLOAD_SIZE + HEADER_SIZE)

            packet_type = int.from_bytes(packet[:2], 'big')
            seq_number = int.from_bytes(packet[2:4], 'big')
            data = packet[4:] 
            if packet_type == SYN:
                self.log_message("rcv", (time.time() - self.start_time), "SYN", int.from_bytes(packet[2:4]), len(packet[4:])) 
                self.expected_seq_number = seq_number + 1
                self.expected_seq_number =  self.expected_seq_number % 65536

                ack_packet = self.create_packet(ACK, self.expected_seq_number)
                self.receiver_socket.sendto(ack_packet, self.sender_address)
                self.log_message("snd", (time.time() - self.start_time), "ACK", int.from_bytes(ack_packet[2:4]), len(ack_packet[4:])) 

            elif packet_type == DATA:

                self.log_message("rcv", (time.time() - self.start_time), "DATA", int.from_bytes(packet[2:4]), len(packet[4:])) 

                if seq_number == self.expected_seq_number:

                    with open(self.filename, 'ab') as file: 

                        file.write(data)
                    self.expected_seq_number += len(data)
                    self.expected_seq_number =  self.expected_seq_number % 65536

                    self.sliding_window = OrderedDict(sorted(self.sliding_window.items()))

                    for seq_no, data_of_seq_no in self.sliding_window.items():
                        if seq_no != self.expected_seq_number:
                            break
                        else:

                            with open(self.filename, 'ab') as file: 

                                file.write(data_of_seq_no)

                            self.expected_seq_number = seq_no + len(data_of_seq_no)
                            self.expected_seq_number = self.expected_seq_number % 65536

                            self.list_of_packet_seq_numbers_to_be_deleted_from_sliding_window.append(seq_no)

                    for i in self.list_of_packet_seq_numbers_to_be_deleted_from_sliding_window:
                        del self.sliding_window[i]

                    self.list_of_packet_seq_numbers_to_be_deleted_from_sliding_window = [] 

                    self.sliding_window = OrderedDict(sorted(self.sliding_window.items()))

                    ack_packet = self.create_packet(ACK, self.expected_seq_number)

                    self.receiver_socket.sendto(ack_packet, self.sender_address) 
                    self.log_message("snd", (time.time() - self.start_time), "ACK", int.from_bytes(ack_packet[2:4]), len(ack_packet[4:])) 
                    self.number_of_original_data_segments_received += 1
                    self.amount_of_original_data_received += len(data)

                else: 

                    self.number_of_duplicate_data_segments_received += 1

                    if (len(self.sliding_window) < self.max_win): 
                        self.sliding_window[seq_number] = data    

                    self.expected_seq_number =  self.expected_seq_number % 65536
                    ack_packet = self.create_packet(ACK, self.expected_seq_number)
                    self.receiver_socket.sendto(ack_packet, self.sender_address)
                    self.log_message("snd", (time.time() - self.start_time), "ACK", int.from_bytes(ack_packet[2:4]), len(ack_packet[4:])) 
                    self.number_of_duplicate_acknowledgments_sent += 1

            elif packet_type == FIN:

                self.log_message("rcv", (time.time() - self.start_time), "FIN", int.from_bytes(packet[2:4]), len(packet[4:])) 

                self.expected_seq_number =  (seq_number + 1) % 65536
                ack_packet = self.create_packet(ACK, self.expected_seq_number)
                self.receiver_socket.sendto(ack_packet, self.sender_address)
                self.log_message("snd", (time.time() - self.start_time), "ACK", int.from_bytes(ack_packet[2:4]), len(ack_packet[4:])) 

                self.connection_teardown_flag = 1
                with open(self.log_filename, 'a') as file:  
                    file.write(f"Original data received:  {self.amount_of_original_data_received}\n")
                    file.write(f"Original segments received: {self.number_of_original_data_segments_received}\n")
                    file.write(f"Dup data segments received: {self.number_of_duplicate_data_segments_received}\n")
                    file.write(f"Dup ack segments sent: {self.number_of_duplicate_acknowledgments_sent}\n")

                self.receiver_socket.close()

            else: 
                pass 

def main():
    if len(sys.argv) != 5:
        print("Usage: python receiver.py receiver_port sender_port txt_file_received max_win")
        sys.exit(1)

    receiver_port = int(sys.argv[1])
    sender_port = int(sys.argv[2])
    txt_file_received = sys.argv[3]
    max_window_size = int(sys.argv[4])

    receiver = Receiver(receiver_port, sender_port, txt_file_received, max_window_size)

    receiver.handle_packets()

if __name__ == "__main__":
    main()