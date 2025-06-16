import socket
import sys
import threading
import time
import datetime
import random
from collections import OrderedDict

SYN = 2
FIN = 3
DATA = 0
ACK = 1
HEADER_SIZE = 4
MSS = 1000

class Sender:
    def __init__(self, sender_port, receiver_port, filename, max_window_size, rto, flp, rlp):
        self.sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sender_socket.bind(('localhost', sender_port))
        self.receiver_address = ('localhost', receiver_port)
        self.filename = filename
        self.max_win = max_window_size/1000
        self.rto = rto / 1000  
        self.flp = flp
        self.rlp = rlp
        self.isn = random.randint(0, 65535) 
        self.next_seq_num = (self.isn + 1) % 65536 

        self.log_filename = f"sender_log.txt"

        self.timeout_timer = threading.Timer(self.rto, self.timeout_thread) 
        self.connection_teardown_event = threading.Event() 
        self.all_data_has_been_read_from_file_flag = 0 
        self.amount_of_original_data_sent_in_bytes = 0
        self.amount_of_original_data_acknowledged_in_bytes = 0
        self.number_of_original_data_segments_sent = 0
        self.number_of_retransmitted_data_segments = 0
        self.number_of_duplicate_acknowledgments_received = 0
        self.number_of_data_segments_dropped = 0
        self.number_of_acknowledgments_dropped = 0

        self.retransmit_oldest_unack_flag = 0
        self.ack_number_of_last_data_packet = -1 
        self.dupackcounter = 0

        self.sliding_window = OrderedDict() 

        self.sliding_window_lock = threading.Lock()

    def log_message(self, direction, time, type, sequence_number, length): 
        with open(self.log_filename, 'a') as file:  
            file.write(f"{direction}    {round(time*1000,2)}    {type}  {sequence_number}   {length}\n")

    def create_packet(self, type, seq_number, data): 
        """ Helper function to create a packet based on the type """
        header = type.to_bytes(2, byteorder='big') + seq_number.to_bytes(2, byteorder='big')
        return header + data

    def connection_setup(self):
        """ Handles sending SYN, FIN segments """

        syn_packet = self.create_packet(SYN, self.isn, b'')

        self.start_time = time.time()
        while(True):
            try: 

                if random.random() > self.flp:
                    self.sender_socket.sendto(syn_packet, self.receiver_address)

                    self.log_message("snd", (time.time() - self.start_time), "SYN", self.isn, 0) 
                else:
                    self.log_message("drp", (time.time() - self.start_time), "SYN", self.isn, 0) 

                    self.rto_timer1 = threading.Timer(self.rto, lambda: None) 
                    self.rto_timer1.start() 
                    self.rto_timer1.join()  
                    self.rto_timer1.cancel()
                    continue
                self.sender_socket.settimeout(self.rto) 

                packet, _ = self.sender_socket.recvfrom(4) 

                ack_type, ack_seq_number = int.from_bytes(packet[0:2], 'big'), int.from_bytes(packet[2:4], 'big')

                ack_seq_number = ack_seq_number % 65536
                if random.random() > self.rlp:
                    self.log_message("rcv", (time.time() - self.start_time), "ACK", ack_seq_number, 0)  
                    if ack_type == ACK and ack_seq_number == self.isn + 1:

                        break 

                else: 
                    self.log_message("drp", (time.time() - self.start_time), "ACK", ack_seq_number, 0)

                    self.rto_timer2 = threading.Timer(self.rto, lambda: None) 

                    self.rto_timer2.start() 
                    self.rto_timer2.join() 
                    self.rto_timer2.cancel()
            except TimeoutError:
                continue

    def timeout_thread(self):
        if (self.sliding_window != {}): 
            if (list(self.sliding_window.keys())[0] == self.oldest_unacknowledged_segment_seqno): 
                self.retransmit_oldest_unack_flag = 1
                self.dupackcounter = 0  

    def send_thread(self):

        self.sender_socket.settimeout(None) 
        with open(self.filename, 'rb') as file: 

            count1 = 0 
            while not self.connection_teardown_event.is_set(): 

                while ((len(self.sliding_window) < self.max_win) and (self.all_data_has_been_read_from_file_flag == 0)): 
                    count1 = count1 + 1
                    file_data = file.read(MSS)
                    if (count1 == 1):
                        self.prev_ack_seq_num = (self.isn + 1) 

                    if len(file_data) < MSS: 
                        self.all_data_has_been_read_from_file_flag = 1
                        self.ack_number_of_last_data_packet = self.next_seq_num              
                        self.ack_number_of_last_data_packet = self.ack_number_of_last_data_packet % 65536 
                        self.length_of_data_of_last_packet = len(file_data) 

                    packet = self.create_packet(DATA, self.next_seq_num, file_data)

                    self.sliding_window[self.next_seq_num] = file_data 
                    self.next_seq_num = (self.next_seq_num + len(file_data)) % 65536  
                    if random.random() > self.flp:
                        self.sender_socket.sendto(packet, self.receiver_address)

                        self.log_message("snd", (time.time() - self.start_time), "DATA", int.from_bytes(packet[2:4]), len(packet[4:]))  
                        self.amount_of_original_data_sent_in_bytes += len(packet[4:])
                        self.number_of_original_data_segments_sent += 1
                    else:
                        self.log_message("drp", (time.time() - self.start_time), "DATA", int.from_bytes(packet[2:4]), len(packet[4:])) 

                        self.all_data_has_been_read_from_file_flag = 0 
                        self.number_of_data_segments_dropped = self.number_of_data_segments_dropped + 1
                        self.amount_of_original_data_sent_in_bytes += len(packet[4:])
                        self.number_of_original_data_segments_sent += 1

                if self.timeout_timer.is_alive():
                    self.timeout_timer.join()  
                    self.timeout_timer.cancel()

                if (self.sliding_window != {}): 

                    self.oldest_unacknowledged_segment_seqno = list(self.sliding_window.keys())[0]

                    self.timeout_timer = threading.Timer(self.rto, self.timeout_thread)

                    self.timeout_timer.start()
                    self.timeout_timer.join()  

    def recv_thread(self):

        while not self.connection_teardown_event.is_set():

            packet, _ = self.sender_socket.recvfrom(MSS) 
            current_ack_seq_no = (int.from_bytes(packet[2:4])) % 65536
            if random.random() > self.rlp:
                self.log_message("rcv", (time.time() - self.start_time), "ACK", int.from_bytes(packet[2:4]), len(packet[4:])) 

                if (current_ack_seq_no == self.ack_number_of_last_data_packet): 

                    self.amount_of_original_data_acknowledged_in_bytes += (current_ack_seq_no - self.prev_ack_seq_num) + self.length_of_data_of_last_packet

                    self.connection_teardown_event.set() 
                    continue              
                elif (current_ack_seq_no == self.prev_ack_seq_num):
                    self.dupackcounter = self.dupackcounter + 1

                    self.number_of_duplicate_acknowledgments_received = self.number_of_duplicate_acknowledgments_received + 1
                    if(self.dupackcounter == 4): 
                        self.retransmit_oldest_unack_flag = 1 

                        self.dupackcounter = 0 

                elif (current_ack_seq_no != self.prev_ack_seq_num): 

                    target_item = current_ack_seq_no

                    keys = list(self.sliding_window.keys())

                    for key in keys:
                        if key == target_item:
                            break
                        del self.sliding_window[key]

                    self.dupackcounter = 0 

                    self.amount_of_original_data_acknowledged_in_bytes += (current_ack_seq_no - self.prev_ack_seq_num)
                    self.prev_ack_seq_num = current_ack_seq_no

                else:
                    pass

            else:
                self.log_message("drp", (time.time() - self.start_time), "ACK", int.from_bytes(packet[2:4]), len(packet[4:])) 

                self.number_of_acknowledgments_dropped = self.number_of_acknowledgments_dropped + 1

    def connection_teardown(self):
        """ Handles sending FIN segments """
        fin_packet = self.create_packet(FIN, (self.ack_number_of_last_data_packet) % 65536, b'') 
        while(True):
            try:

                if random.random() > self.flp:
                    self.sender_socket.sendto(fin_packet, self.receiver_address)
                    self.log_message("snd", (time.time() - self.start_time), "FIN", self.ack_number_of_last_data_packet, 0) 
                else:
                    self.log_message("drp", (time.time() - self.start_time), "FIN", self.ack_number_of_last_data_packet, 0) 

                    self.rto_timer = threading.Timer(self.rto, lambda: None)

                    self.rto_timer.start() 
                    self.rto_timer.join()  
                    self.rto_timer.cancel()
                    continue
                self.sender_socket.settimeout(self.rto) 

                packet, _ = self.sender_socket.recvfrom(4) 

                ack_type, ack_seq_number = int.from_bytes(packet[0:2], 'big'), int.from_bytes(packet[2:4], 'big')

                ack_seq_number = ack_seq_number % 65536
                if random.random() > self.rlp:
                    self.log_message("rcv", (time.time() - self.start_time), "ACK", ack_seq_number, 0)  
                    if ack_type == ACK and ack_seq_number == self.ack_number_of_last_data_packet + 1:

                        self.sender_socket.settimeout(None) 
                        break 

                else: 
                    self.log_message("drp", (time.time() - self.start_time), "ACK", ack_seq_number, 0)

                    self.rto_timer = threading.Timer(self.rto, lambda: None)

                    self.rto_timer.start() 
                    self.rto_timer.join() 
                    self.rto_timer.cancel()
            except TimeoutError:

                break 

        with open(self.log_filename, 'a') as file:  
            file.write(f"Original data sent:  {self.amount_of_original_data_sent_in_bytes}\n")
            file.write(f"Original data acked: {self.amount_of_original_data_acknowledged_in_bytes}\n")
            file.write(f"Original segments sent: {self.number_of_original_data_segments_sent}\n")
            file.write(f"Retransmitted segments: {self.number_of_retransmitted_data_segments}\n")
            file.write(f"Dup acks received: {self.number_of_duplicate_acknowledgments_received}\n")
            file.write(f"Data segments dropped: {self.number_of_data_segments_dropped}\n")
            file.write(f"Ack segments dropped: {self.number_of_acknowledgments_dropped}\n")

        self.sender_socket.close()  

def main():
    if len(sys.argv) != 8:
        print("Usage: python sender.py sender_port receiver_port txt_file_to_send max_win rto flp rlp")
        sys.exit(1)

    sender_port = int(sys.argv[1])
    receiver_port = int(sys.argv[2])
    filename = sys.argv[3]
    max_window_size = int(sys.argv[4])
    rto = int(sys.argv[5])
    flp = float(sys.argv[6])
    rlp = float(sys.argv[7])

    sender = Sender(sender_port, receiver_port, filename, max_window_size, rto, flp, rlp)    

    sender.connection_setup()

    send  = threading.Thread(target = sender.send_thread)
    receive = threading.Thread(target = sender.recv_thread)
    send.start()
    receive.start()

    while not sender.connection_teardown_event.is_set():

        while (sender.retransmit_oldest_unack_flag == 1):

            if (sender.sliding_window != {}): 
                oldest_unacked_packet_seqno = (list(sender.sliding_window.keys())[0]) % 65536
                oldest_unacked_packet_data = sender.sliding_window[oldest_unacked_packet_seqno]
                packet = sender.create_packet(DATA, oldest_unacked_packet_seqno, oldest_unacked_packet_data)
                if random.random() > sender.flp:
                    sender.sender_socket.sendto(packet, sender.receiver_address)

                    sender.log_message("snd", (time.time() - sender.start_time), "DATA", int.from_bytes(packet[2:4]), len(packet[4:]))  
                    sender.number_of_retransmitted_data_segments = sender.number_of_retransmitted_data_segments + 1
                else:
                    sender.log_message("drp", (time.time() - sender.start_time), "DATA", int.from_bytes(packet[2:4]), len(packet[4:])) 

                    sender.number_of_retransmitted_data_segments = sender.number_of_retransmitted_data_segments + 1
                    sender.number_of_data_segments_dropped += 1
                sender.retransmit_oldest_unack_flag = 0 

    send.join()
    receive.join()

    sender.connection_teardown()

if __name__ == "__main__": 
    main()