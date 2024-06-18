import socket
import threading
import sys
import time
import queue
import os


class Client:
    def __init__(self, username, connection, address):
        self.username = username
        self.connection = connection
        self.address = address
        self.kicked = False
        self.in_queue = True
        self.remaining_time = 100 # remaining time before AFK
        self.muted = False
        self.mute_duration = 0


class Channel:
    def __init__(self, name, port, capacity):
        self.name = name
        self.port = port
        self.capacity = capacity
        self.queue = queue.Queue()
        self.clients = []


def parse_config(config_file: str) -> list:
    """
    Parses lines from a given configuration file and VALIDATE the format of each line. The 
    function validates each part and if valid returns a list of tuples where each tuple contains
    (channel_name, channel_port, channel_capacity). The function also ensures that there are no 
    duplicate channel names or ports. if not valid, exit with status code 1.
    Status: TEST
    Args:
        config_file (str): The path to the configuration file (e.g, config_01.txt).
    Returns:
        list: A list of tuples where each tuple contains:
        (channel_name, channel_port, and channel_capacity)
    Raises:
        SystemExit: If there is an error in the configuration file format.
    """
    # Write your code here...
    try:
        with open(config_file) as f:
            channels = []
            lines = f.readlines()

            for line in lines:
                line = line.strip().split(" ")

                if (len(line) != 4 or # must be 4 arguments
                    line[0] != "channel" or # first argument is not "channel"
                    not line[1].isalpha() or # channel name contains numbers or characters
                    not line[2].isnumeric() or # port number is not numeric
                    int(line[2]) == 0 or # using ephemoral port
                    not line[3].isnumeric() or # capacity is not numeric
                    not (1 <= int(line[3]) <= 5)): # capacity is not between 1 and 5 inclusive
                    raise Exception
                
                channels.append((line[1], int(line[2]), int(line[3])))
            
            return channels
            
    except Exception as e:
        print('Exiting with code 1...')
        sys.exit(1)

def get_channels_dictionary(parsed_lines) -> dict:
    """
    Creates a dictionary of Channel objects from parsed lines.
    Status: Given
    Args:
        parsed_lines (list): A list of tuples where each tuple contains:
        (channel_name, channel_port, and channel_capacity)
    Returns:
        dict: A dictionary of Channel objects where the key is the channel name.
    """
    channels = {}

    for channel_name, channel_port, channel_capacity in parsed_lines:
        channels[channel_name] = Channel(channel_name, channel_port, channel_capacity)

    return channels

def quit_client(client, channel) -> None:
    """
    Implement client quitting function
    Status: TEST
    """
    quit_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has left the channel."

    # if client is in queue
    if client.in_queue:
        # Write your code here...
        # remove, close connection, and print quit message in the server.
        channel.queue = remove_item(channel.queue, client)
        client.connection.close()
        print(quit_msg, flush=True)

        # broadcast queue update message to all the clients in the queue.
        queued_clients = list(channel.queue.queue)
        for queued_client in queued_clients:
            queue_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You are in the waiting queue and there are {queued_clients.index(queued_client)} user(s) ahead of you."
            queued_client.connection.send(queue_msg.encode())

    # if client is in channel
    else:
        # Write your code here...
        # remove client from the channel, close connection, and broadcast quit message to all clients.
        channel.clients.remove(client)
        client.connection.close()
        broadcast_in_channel(client, channel, quit_msg)
        print(quit_msg, flush=True)

def send_client(client, channel, msg) -> None:
    """
    Implement file sending function, if args for /send are valid.
    Else print appropriate message and return.
    Status: TEST
    """
    # Write your code here...
    # if in queue, do nothing
    if client.in_queue:
        return
    else:
        # if muted, send mute message to the client
        if client.muted:
            mute_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You are still muted for {client.mute_duration} seconds."
            client.connection.send(mute_msg.encode())

        # if not muted, process the file sending
        else:
            # validate the command structure
            cmd = msg.split(' ')
            if ((len(cmd) != 3) or cmd[0] != "/send"):
                return
            
            # check for target existance
            target = cmd[1]
            target_client = None
            for search_client in channel.clients:
                if search_client.username == target:
                    target_client = search_client
                    break
            
            if not (target_client):
                target_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {target} is not here."
                client.connection.send(target_msg.encode())
                return
            
            # check for file existence
            file_path = cmd[2]
            try:
                with open(file_path, 'r') as file:
                    target_client.connection.send(f"FILE {file_path}".encode())
                    while True:
                        data = file.read()
                        if not data:
                            break
                        target_client.connection.sendall(data.encode())
                    
                    client_success_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You sent {file_path} to {target}."
                    server_success_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} sent {file_path} to {target}."
                    client.connection.send(client_success_msg.encode())
                    print(server_success_msg, flush=True)
            except FileNotFoundError:
                file_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {file_path} does not exist."
                client.connection.send(file_msg.encode())

            # check if receiver is in the channel, and send the file

def list_clients(client, channels) -> None:
    """
    List all channels and their capacity
    Status: TEST
    """
    # Write your code here...
    channel_info = []
    for channel in channels.values():
        msg = f"[Channel] {channel.name} {channel.port} Capacity: {len(channel.clients)}/ {channel.capacity}, Queue: {channel.queue.qsize()}."
        channel_info.append(msg)
    
    client.connection.send(("\n".join(channel_info)).encode())

def whisper_client(client, channel, msg) -> None:
    """
    Implement whisper function, if args for /whisper are valid.
    Else print appropriate message and return.
    Status: TEST
    """
    # Write your code here...
    # if in queue, do nothing
    if client.in_queue:
        return
    else:
        # if muted, send mute message to the client
        if client.muted:
            mute_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You are still muted for {client.mute_duration} seconds."
            client.connection.send(mute_msg.encode())
        else:
            # validate the command structure
            cmd = msg.split(' ')
            if ((len(cmd) != 3) or cmd[0] != "/whisper"):
                return

            # validate if the target user is in the channel
            target = cmd[1]
            target_client = None
            for search_client in channel.clients:
                if search_client.username == target:
                    target_client = search_client
                    break
            
            # if target user is in the channel, send the whisper message
            if not (target_client):
                target_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {target} is not here."
                client.connection.send(target_msg.encode())
                return
            
            target_msg = f"[{client.username} whispers to you: ({time.strftime('%H:%M:%S')})] {cmd[2]}"
            target_client.connection.send(target_msg.encode())

            # print whisper server message
            server_msg = f"[{client.username} whispers to {target}: ({time.strftime('%H:%M:%S')})] {cmd[2]}"
            print(server_msg, flush=True)

def switch_channel(client, channel, msg, channels) -> bool:
    """
    Implement channel switching function, if args for /switch are valid.
    Else print appropriate message and return.

    Returns: bool
    Status: TEST
    """
    # Write your code here...
    # validate the command structure
    cmd = msg.split(' ')
    if ((len(cmd) != 2) or cmd[0] != "/switch"):
        return False
    # check if the new channel exists
    requested_channel_name = cmd[1]
    if not requested_channel_name in channels.keys():
        no_such_channel = f"[Server message ({time.strftime('%H:%M:%S')})] {requested_channel_name} does not exist."
        client.connection.send(no_such_channel.encode())
        return False

    # check if there is a client with the same username in the new channel
    requested_channel = channels[requested_channel_name]
    for other_client in requested_channel.clients:
        if other_client.username == client.username:
            user_exists = f"[Server message ({time.strftime('%H:%M:%S')})] {requested_channel_name} already has a user with username {client.username}."
            client.connection.send(user_exists.encode())
            return False
    
    server_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has left the channel."

    # if all checks are correct, and client in queue
    if client.in_queue:
        # remove client from current channel queue
        channel.queue = remove_item(channel.queue, client)

        # broadcast queue update message to all clients in the current channel
        queued_clients = list(channel.queue.queue)
        for queued_client in queued_clients:
            queue_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You are in the waiting queue and there are {queued_clients.index(queued_client)} user(s) ahead of you."
            queued_client.connection.send(queue_msg.encode())

        # tell client to connect to new channel and close connection
        client.connection.send(f"SWITCH {requested_channel.port}".encode())
        client.connection.close()

        print(server_msg, flush=True)

    # if all checks are correct, and client in channel
    else:
        # remove client from current channel
        channel.clients.remove(client)
        broadcast_in_channel(client, channel, server_msg)
        print(server_msg, flush=True)

        # tell client to connect to new channel and close connection
        client.connection.send(f"SWITCH {requested_channel.port}".encode())
        client.connection.close()

    return True

def broadcast_in_channel(client, channel, msg) -> None:
    """
    Broadcast a message to all clients in the channel.
    Status: TEST
    """
    # Write your code here...
    # if in queue, do nothing
    if client.in_queue:
        return

    # if muted, send mute message to the client
    elif client.muted:
        mute_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You are still muted for {client.mute_duration} seconds."
        client.connection.send(mute_msg.encode())

    # broadcast message to all clients in the channel
    else:
        for other_client in channel.clients:
            other_client.connection.send(msg.encode())

def client_handler(client, channel, channels) -> None:
    """
    Handles incoming messages from a client in a channel. Supports commands to quit, send, switch, whisper, and list channels. 
    Manages client's mute status and remaining time. Handles client disconnection and exceptions during message processing.
    Status: TEST (check the "# Write your code here..." block in Exception)
    Args:
        client (Client): The client to handle.
        channel (Channel): The channel in which the client is.
        channels (dict): A dictionary of all channels.
    """
    while True:
        if client.kicked:
            break
        try:
            msg = client.connection.recv(1024).decode()

            # check message for client commands
            if msg.startswith("/quit"):
                quit_client(client, channel)
                break
            elif msg.startswith("/send"):
                send_client(client, channel, msg)
            elif msg.startswith("/list"):
                list_clients(client, channels)
            elif msg.startswith("/whisper"):
                whisper_client(client, channel, msg)
            elif msg.startswith("/switch"):
                is_valid = switch_channel(client, channel, msg, channels)
                if is_valid:
                    break
                else:
                    continue

            # if not a command, broadcast message to all clients in the channel
            else:
                new_msg = f"[{client.username} ({time.strftime('%H:%M:%S')})] {msg}"
                print(new_msg, flush=True)
                broadcast_in_channel(client, channel, new_msg)

            # reset remaining time before AFK
            if not client.muted:
                client.remaining_time = 100
        except EOFError:
            continue
        except OSError:
            break
        except Exception as e:
            print(f"Error in client handler: {e}")
            # remove client from the channel, close connection
            # Write your code here...
            channel.clients.remove(client)
            client.connection.close()

            break

def check_duplicate_username(username, channel, conn) -> bool:
    """
    Check if a username is already in a channel or its queue.
    Status: TEST
    """
    # Write your code here...
    for client in list(channel.queue.queue):
        if client.username == username:
            conn.close()
            return False
    
    for client in channel.clients:
        if client.username == username:
            conn.close()
            return False
        
    return True

def position_client(channel, conn, username, new_client) -> None:
    """
    Place a client in a channel or queue based on the channel's capacity.
    Status: TEST
    """
    # Write your code here...
    if len(channel.clients) < channel.capacity and channel.queue.empty():
        # put client in channel and reset remaining time before AFK
        channel.clients.append(new_client)
        new_client.in_queue = False
        new_client.remaining_time = 100

        channel_join_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {username} has joined the channel."
        server_join_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {username} has joined the {channel.name} channel."

        broadcast_in_channel(new_client, channel, channel_join_msg)
        print(server_join_msg, flush=True)
    else:
        # put client in queue
        channel.queue.put(new_client)
        queue_join_msg = f"[Server message ({time.strftime('%H:%M:%S')})] Welcome to the {channel.name} waiting room {username}."
        conn.send(queue_join_msg.encode())

        queued_clients = list(channel.queue.queue)
        queue_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You are in the waiting queue and there are {queued_clients.index(new_client)} user(s) ahead of you."
        conn.send(queue_msg.encode())

def channel_handler(channel, channels) -> None:
    """
    Starts a chat server, manage channels, respective queues, and incoming clients.
    This initiates different threads for chanel queue processing and client handling.
    Status: Given
    Args:
        channel (Channel): The channel for which to start the server.
    Raises:
        EOFError: If there is an error in the client-server communication.
    """
    # Initialize server socket, bind, and listen
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("localhost", channel.port))
    server_socket.listen(channel.capacity)

    # launch a thread to process client queue
    queue_thread = threading.Thread(target=process_queue, args=(channel,))
    queue_thread.start()

    while True:
        try:
            # accept a client connection
            conn, addr = server_socket.accept()
            username = conn.recv(1024).decode()

            # check duplicate username in channel and channel's queue
            is_valid = check_duplicate_username(username, channel, conn)
            if not is_valid: continue

            welcome_msg = f"[Server message ({time.strftime('%H:%M:%S')})] Welcome to the {channel.name} channel, {username}."
            conn.send(welcome_msg.encode())
            time.sleep(0.1)
            new_client = Client(username, conn, addr)

            # position client in channel or queue
            position_client(channel, conn, username, new_client)

            # Create a client thread for each connected client, whether they are in the channel or queue
            client_thread = threading.Thread(target=client_handler, args=(new_client, channel, channels))
            client_thread.start()
        except EOFError:
            continue

def remove_item(q, item_to_remove) -> queue.Queue:
    """
    Remove item from queue
    Status: Given
    Args:
        q (queue.Queue): The queue to remove the item from.
        item_to_remove (Client): The item to remove from the queue.
    Returns:
        queue.Queue: The queue with the item removed.
    """
    new_q = queue.Queue()
    while not q.empty():
        current_item = q.get()
        if current_item != item_to_remove:
            new_q.put(current_item)

    return new_q

def process_queue(channel) -> None:
    """
    Processes the queue of clients for a channel in an infinite loop. If the channel is not full, 
    it dequeues a client, adds them to the channel, and updates their status. It then sends updates 
    to all clients in the channel and queue. The function handles EOFError exceptions and sleeps for 
    1 second between iterations.
    Status: TEST
    Args:
        channel (Channel): The channel whose queue to process.
    Returns:
        None
    """
    # Write your code here...
    while True:
        try:
            if not channel.queue.empty() and len(channel.clients) < channel.capacity:
                # Dequeue a client from the queue and add them to the channel
                client = channel.queue.get()
                client.in_queue = False
                channel.clients.append(client)

                # Send join message to all clients in the channel
                join_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} has joined the {channel.name} channel."
                broadcast_in_channel(client, channel, join_msg)

                # Update the queue messages for remaining clients in the queue
                queued_clients = list(channel.queue.queue)
                for queued_client in queued_clients:
                    queue_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You are in the waiting queue and there are {queued_clients.index(queued_client)} user(s) ahead of you."
                    queued_client.connection.send(queue_msg.encode())
                
                # Reset the remaining time to 100 before AFK
                client.remaining_time = 100
                
                time.sleep(1)
        except EOFError:
            continue

def kick_user(command, channels) -> None:
    """
    Implement /kick function
    Status: TEST
    Args:
        command (str): The command to kick a user from a channel.
        channels (dict): A dictionary of all channels.
    Returns:
        None
    """
    # Write your code here...
    # validate command structure
    cmd = command.split(' ')
    if ((len(cmd) != 3) or cmd[0] != "/kick"):
        return
    
    # check if the channel exists in the dictionary
    channel_name = cmd[1]
    if not channel_name in channels.keys():
        no_such_channel = f"[Server message ({time.strftime('%H:%M:%S')})] {channel_name} does not exist."
        print(no_such_channel, flush=True)
        return

    # if channel exists, check if the user is in the channel
    username = cmd[2]
    channel = channels[channel_name]
    for client in channel.clients:
        # if user is in the channel, kick the user
        if client.username == username:
            client.connection.close()
            channel.clients.remove(client)
            
            broadcast_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {username} has left the channel."
            broadcast_in_channel(client, channel, broadcast_msg)

            server_msg = f"[Server message ({time.strftime('%H:%M:%S')})] Kicked {username}."
            print(server_msg, flush=True)
            return
    
    # if user is not in the channel, print error message
    no_such_user = f"[Server message ({time.strftime('%H:%M:%S')})] {username} is not in {channel_name}."
    print(no_such_user, flush=True)

def empty(command, channels) -> None:
    """
    Implement /empty function
    Status: TEST
    Args:
        command (str): The command to empty a channel.
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # validate the command structure
    cmd = command.split(' ')
    if ((len(cmd) != 2) or cmd[0] != "/empty"):
        return

    # check if the channel exists in the server
    channel_name = cmd[1]
    if not channel_name in channels.keys():
        no_such_channel = f"[Server message ({time.strftime('%H:%M:%S')})] {channel_name} does not exist."
        print(no_such_channel, flush=True)
        return

    # if the channel exists, close connections of all clients in the channel
    channel = channels[channel_name]
    for client in channel.clients:
        client.connection.close()

    server_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {channel_name} has been emptied."
    print(server_msg, flush=True)

def mute_user(command, channels) -> None:
    """
    Implement /mute function
    Status: TEST
    Args:
        command (str): The command to mute a user in a channel.
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # validate the command structure
    cmd = command.split(' ')
    if ((len(cmd) != 4) or cmd[0] != "/mute"):
        return
    
    # check if the mute time is valid
    mute_time = int(cmd[3])
    valid_mute_time = True
    if mute_time < 1:
        invalid_mute = f"[Server message ({time.strftime('%H:%M:%S')})] Invalid mute time."
        valid_mute_time = False

    # check if the channel exists in the server
    channel_name = cmd[1]
    username = cmd[2]
    no_such_user = f"[Server message ({time.strftime('%H:%M:%S')})] {username} does not exist."
    
    if not channel_name in channels.keys():
        if not valid_mute_time:
            print(invalid_mute, flush=True)

        print(no_such_user, flush=True)
        return

    # if the channel exists, check if the user is in the channel
    channel = channels[channel_name]
    target_client = None
    user_exists = False
    
    for client in channel.clients:
        if username == client.username:
            target_client = client
            user_exists = True

    # if user is in the channel, mute it and send messages to all clients
    if user_exists and valid_mute_time:
        user_exists = True
        target_client.muted = True
        target_client.mute_duration = mute_time

        client_msg = f"[Server message ({time.strftime('%H:%M:%S')})] You have been muted for {mute_time} seconds."
        server_msg = f"[Server message ({time.strftime('%H:%M:%S')})] Muted {username} for {mute_time} seconds."
        channel_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {username} has been muted for {mute_time} seconds."

        target_client.connection.send(client_msg.encode())
        print(server_msg, flush=True)
        
        for other_client in channel.clients:
            if username != other_client.username:
                other_client.connection.send(channel_msg.encode())
        return

    # if user is not in the channel, print error message
    if not valid_mute_time:
        print(invalid_mute, flush=True)

    if not user_exists:
        print(no_such_user, flush=True)

def shutdown(channels) -> None:
    """
    Implement /shutdown function
    Status: TEST
    Args:
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    # close connections of all clients in all channels and exit the server
    for channel in channels.values():
        for client in channel.clients:
            client.connection.close()

    # end of code insertion, keep the os._exit(0) as it is
    os._exit(0)

def server_commands(channels) -> None:
    """
    Implement commands to kick a user, empty a channel, mute a user, and shutdown the server.
    Each command has its own validation and error handling. 
    Status: Given
    Args:
        channels (dict): A dictionary of all channels.
    Returns:
        None
    """
    while True:
        try:
            command = input()
            if command.startswith('/kick'):
                kick_user(command, channels)
            elif command.startswith("/empty"):
                empty(command, channels)
            elif command.startswith("/mute"):
                mute_user(command, channels)
            elif command == "/shutdown":
                shutdown(channels)
            else:
                continue
        except EOFError:
            continue
        except Exception as e:
            print(f"{e}")
            sys.exit(1)

def check_inactive_clients(channels) -> None:
    """
    Continuously manages clients in all channels. Checks if a client is muted, in queue, or has run out of time. 
    If a client's time is up, they are removed from the channel and their connection is closed. 
    A server message is sent to all clients in the channel. The function also handles EOFError exceptions.
    Status: TEST
    Args:
        channels (dict): A dictionary of all channels.
    """
    # Write your code here...
    try:
    # parse through all the clients in all the channels
        for channel in channels.values():
            for client in channel.clients:
    # if client is muted or in queue, do nothing
                    
    # remove client from the channel and close connection, print AFK message
                if not client.muted and not client.in_queue:
                    channel.clients.remove(client)
                    client.connection.close()
                    
                    afk_msg = f"[Server message ({time.strftime('%H:%M:%S')})] {client.username} went AFK."
                    print(afk_msg, flush=True)
    
    # if client is not muted, decrement remaining time
                if not client.muted:
                    client.remaining_time -= 1

    except EOFError as err:
        print(err)

def handle_mute_durations(channels) -> None:
    """
    Continuously manages the mute status of clients in all channels. If a client's mute duration has expired, 
    their mute status is lifted. If a client is still muted, their mute duration is decremented. 
    The function sleeps for 0.99 seconds between iterations and handles EOFError exceptions.
    Status: Given
    Args:
        channels (dict): A dictionary of all channels.
    """
    while True:
        try:
            for channel_name in channels:
                channel = channels[channel_name]
                for client in channel.clients:
                    if client.mute_duration <= 0:
                        client.muted = False
                        client.mute_duration = 0
                    if client.muted and client.mute_duration > 0:
                        client.mute_duration -= 1
            time.sleep(0.99)
        except EOFError:
            continue

def main():
    try:
        if len(sys.argv) != 2:
            print("Usage: python3 chatserver.py configfile")
            sys.exit(1)

        config_file = sys.argv[1]

        # parsing and creating channels
        parsed_lines = parse_config(config_file)
        channels = get_channels_dictionary(parsed_lines)

        # creating individual threads to handle channels connections
        for _, channel in channels.items():
            thread = threading.Thread(target=channel_handler, args=(channel, channels))
            thread.start()

        server_commands_thread = threading.Thread(target=server_commands, args=(channels,))
        server_commands_thread.start()

        inactive_clients_thread = threading.Thread(target=check_inactive_clients, args=(channels,))
        inactive_clients_thread.start()

        mute_duration_thread = threading.Thread(target=handle_mute_durations, args=(channels,))
        mute_duration_thread.start()
    except KeyboardInterrupt:
        print("Ctrl + C Pressed. Exiting...")
        os._exit(0)


if __name__ == "__main__":
    main()
