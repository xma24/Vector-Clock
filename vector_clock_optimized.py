from multiprocessing import Process, Pipe
import os
import threading
import time
import numpy as np

process_number = 3
event_number = 3


def sending_events_thread(pid, event_queue, start, pipe_list_local, process_id, event_list, time_stamp, sending_indicator):
    print("P%s:PID%s " % (str(process_id), str(pid)))
    print("All the events in the list: ", event_list)

    event_index_process = [0, 0, 0]

    start_process = 1
    stop_sending = 0

    while True:
        if stop_sending == 0:
            time.sleep(3)
            if process_id == 1 and start_process == 1:
                start_process = 0
                time_stamp[process_id - 1] += 1
                event_index = event_index_process[process_id - 1]
                message_to_send = create_event(pid, event_list[event_index], time_stamp, process_id)
                event_queue.append(message_to_send)
                send_messages(pipe_list_local, process_id, message_to_send)
                if event_index_process[process_id - 1] == process_number - 1:
                    event_index_process[process_id - 1] += 1
                    sending_indicator[0] = 0
                else:
                    event_index_process[process_id - 1] += 1

            if sending_indicator[0] == 1:
                time_stamp[process_id - 1] += 1
                event_index = event_index_process[process_id - 1]
                message_to_send = create_event(pid, event_list[event_index], time_stamp, process_id)
                send_messages(pipe_list_local, process_id, message_to_send)
                event_queue.append(message_to_send)
                if event_index_process[process_id - 1] >= process_number - 1:
                    event_index_process[process_id - 1] += 1
                    stop_sending = 1
                else:
                    event_index_process[process_id - 1] += 1
                sending_indicator[0] = 0


def create_event(pid, event_message, time_stamp, process_id):
    event = str(pid) + "." + str(event_message)
    timestamp = str(time_stamp)
    return event, timestamp, process_id


def send_messages(pipe_list_local, process_id, event):
    if process_id == 1:
        pipe_list_local[0][0].send(event)
        pipe_list_local[2][0].send(event)
    elif process_id == 2:
        pipe_list_local[0][1].send(event)
        pipe_list_local[1][0].send(event)
    elif process_id == 3:
        pipe_list_local[2][1].send(event)
        pipe_list_local[1][1].send(event)


def receive_messages(pipe_list_local, process_id, start):
    received_events = []
    time.sleep(7)
    print("")
    print("")
    print("process_id: ", process_id)

    if start == 1:
        if process_id == 1:
            received_events.append(pipe_list_local[0][0].recv())
            received_events.append(pipe_list_local[2][0].recv())
        elif process_id == 2:
            received_events.append(pipe_list_local[0][1].recv())
        elif process_id == 3:
            received_events.append(pipe_list_local[2][1].recv())
            received_events.append(pipe_list_local[1][1].recv())
        else:
            print("Process ID is not valid.")
    elif start == 0:
        if process_id == 1:
            received_events.append(pipe_list_local[0][0].recv())
            received_events.append(pipe_list_local[2][0].recv())
        elif process_id == 2:
            received_events.append(pipe_list_local[0][1].recv())
            received_events.append(pipe_list_local[1][0].recv())
        elif process_id == 3:
            received_events.append(pipe_list_local[2][1].recv())
            received_events.append(pipe_list_local[1][1].recv())
        else:
            print("Process ID is not valid.")
    elif start == 2:
        if process_id == 1:
            received_events.append(pipe_list_local[0][0].recv())
            received_events.append(pipe_list_local[2][0].recv())
        elif process_id == 2:
            received_events.append(pipe_list_local[1][0].recv())
        elif process_id == 3:
            pass
        else:
            print("Process ID is not valid.")
    return received_events


def check_rule_deley_list(deley_event_buffer, event_queue, time_stamp):
    updated_index = []
    if not len(deley_event_buffer) == 0:
        for deley_event_i in range(len(deley_event_buffer)):
            deley_indicator = 0
            print("deley_event_buffer: ", deley_event_buffer)
            time_stamp_list = deley_event_buffer[deley_event_i][1].split("[")[1].split("]")[0].split(",")

            source_process = deley_event_buffer[deley_event_i][2] - 1
            if not time_stamp[source_process] - int(time_stamp_list[source_process]) == -1:
                deley_indicator += 1

            for time_i in range(process_number):
                if not time_i == deley_event_buffer[deley_event_i][2] - 1:
                    if time_stamp[time_i] != int(time_stamp_list[time_i]):
                        deley_indicator += 1

            if deley_indicator == 0:
                time_stamp[source_process] += 1
                updated_index.append(deley_event_i)
                event_queue.append(deley_event_buffer[deley_event_i])
    return updated_index


def deliver_event(event_to_deliver, process_id):
    current_procee_id = process_id
    sending_process_id = event_to_deliver[2]
    event_id = event_to_deliver[0].split(".")[1]
    message_to_print = str(current_procee_id) + ":" + str(sending_process_id) + "." + str(event_id)
    print(message_to_print)


def communication_thread(pid, event_queue, deley_event_buffer, pipe_list_local, process_id,time_stamp, sending_indicator, start):
    receive_number = 1
    print_event_index = 0
    while True:
        # the process will be blocked by the receive function
        received_events_local = receive_messages(pipe_list_local, process_id, start[0])
        receive_number += 1
        if receive_number == process_number + 1:
            start[0] = 2
        else:
            start[0] = 0

        print("time_stamp: ", time_stamp)
        print("received events " + str(pid), received_events_local)

        if not len(deley_event_buffer) == 0:
            updated_event_list = check_rule_deley_list(deley_event_buffer, event_queue, time_stamp)

            print("deley_event_buffer: ", deley_event_buffer)

            new_delay_list = []
            for envent_idx in range(len(deley_event_buffer)):
                if envent_idx not in updated_event_list:
                    new_delay_list.append(deley_event_buffer[envent_idx])
            print("new_delay_list: ", new_delay_list)
            deley_event_buffer = new_delay_list

        for event_i in range(len(received_events_local)):
            deley_indicator = 0
            time_stamp_list = received_events_local[event_i][1].split("[")[1].split("]")[0].split(",")
            source_process = received_events_local[event_i][2] - 1
            if not time_stamp[source_process] - int(time_stamp_list[source_process]) == -1:
                deley_indicator += 1

            for time_i in range(process_number):
                if not time_i == received_events_local[event_i][2] - 1:
                    if time_stamp[time_i] < int(time_stamp_list[time_i]):
                        deley_indicator += 1

            if deley_indicator > 0:
                deley_event_buffer.append(received_events_local[event_i])
            else:
                time_stamp[source_process] += 1
                event_queue.append(received_events_local[event_i])

        if not len(deley_event_buffer) == 0:
            updated_event_list = check_rule_deley_list(deley_event_buffer, event_queue, time_stamp)

            new_delay_list = []
            for envent_idx in range(len(deley_event_buffer)):
                if envent_idx not in updated_event_list:
                    new_delay_list.append(deley_event_buffer[envent_idx])
            deley_event_buffer = new_delay_list

        sending_indicator[0] = 1
        print("event queue: ", event_queue)
        start_event_index = print_event_index
        for print_event_i in range(start_event_index, len(event_queue)):
            deliver_event(event_queue[print_event_i], process_id)
            print_event_index += 1
        print("")


def process1(pipe_list_local):
    current_pid = os.getpid()
    process_id = 1
    event_list_process_1 = [x for x in range(0, event_number)]
    time_stamp = [0, 0, 0]
    event_queue = []
    deley_event_buffer = []
    sending_indicator = [0]
    start = [1]

    thread_send = threading.Thread(target=sending_events_thread, args=(
        current_pid, event_queue, start, pipe_list_local, process_id, event_list_process_1, time_stamp,
        sending_indicator))

    thread_communicate = threading.Thread(target=communication_thread,
                                          args=(current_pid, event_queue, deley_event_buffer,
                                                pipe_list_local, process_id, time_stamp, sending_indicator, start))

    thread_send.start()
    thread_communicate.start()

    thread_send.join()
    thread_communicate.join()


def process2(pipe_list_local):
    current_pid = os.getpid()
    event_list_process_2 = [x for x in range(20, 20 + event_number)]
    process_id = 2
    time_stamp = [0, 0, 0]
    event_queue = []
    deley_event_buffer = []
    sending_indicator = [0]
    start = [1]

    time.sleep(2)

    thread_send = threading.Thread(target=sending_events_thread, args=(
        current_pid, event_queue, start, pipe_list_local, process_id, event_list_process_2, time_stamp,
        sending_indicator))

    thread_communicate = threading.Thread(target=communication_thread,
                                          args=(
                                              current_pid, event_queue, deley_event_buffer,
                                              pipe_list_local, process_id,
                                              time_stamp, sending_indicator, start))

    thread_send.start()
    thread_communicate.start()

    thread_send.join()
    thread_communicate.join()


def process3(pipe_list_local):
    current_pid = os.getpid()
    event_list_process_3 = [x for x in range(40, 40 + event_number)]
    process_id = 3
    time_stamp = [0, 0, 0]
    event_queue = []
    deley_event_buffer = []
    sending_indicator = [0]
    start = [1]

    time.sleep(5)

    thread_send = threading.Thread(target=sending_events_thread, args=(
        current_pid, event_queue, start, pipe_list_local, process_id, event_list_process_3, time_stamp,
        sending_indicator))

    thread_communicate = threading.Thread(target=communication_thread,
                                          args=(
                                              current_pid, event_queue, deley_event_buffer,
                                              pipe_list_local, process_id,
                                              time_stamp, sending_indicator, start))

    thread_send.start()
    thread_communicate.start()

    thread_send.join()
    thread_communicate.join()


pipe_list = []

for pipe_index in range(process_number):
    (pipe_send, pipe_recv) = Pipe()
    pipe_list.append((pipe_send, pipe_recv))

print(pipe_list)

P1 = Process(target=process1, args=(pipe_list,))
P2 = Process(target=process2, args=(pipe_list,))
P3 = Process(target=process3, args=(pipe_list,))

P1.start()
P2.start()
P3.start()

P1.join()
P2.join()
P3.join()
