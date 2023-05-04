# MIT License
# 
# Copyright (c) 2023 Dalton Kinney
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys, argparse, redis, signal

def getargs(parser):
    """
    getargs Get arguments from the command line
    
    :parser: Parser object that holds arguments

    return: Returns parsed arguments
    """
    parser.add_argument('--help', '-h', action='help', help='Displays this information')
    parser.add_argument('serverip', metavar="<serverip>", help="Redis Ip address")
    parser.add_argument('serverport', type=int, metavar="<port>", help="Redis server port")
    parser.add_argument('database', type=int, metavar="<DB 0-15>", default=0, nargs='?', help="Redis Database to subscribe to (Default DB 0)")

    return parser.parse_args()

def redis_connect(ip, port, db):
    """
    redis_connect Connects to the redis server 
    
    :ip:   Ip address of Redis server
    :port: Port number " " 
    :db:   Redis database to access

    return: Returns redis server object if successful, false otherwise
    """
    r = redis.Redis(
        host=ip,
        port=port,
        db=0)

    try: # to ping the Redis server to ensure we have a valid connection
        r.ping()
        return r
    except Exception as e:
        sys.stderr.write("Error: %s\n" % e) # Quite informative error message
        sys.exit(1)

def event_handler(msg):
    global g_event, g_command, g_key
    if msg['type'] == 'pmessage':
        if not g_event:
            g_event = 1                         # Received our first event, COMMAND
            g_command = msg['data'].decode("utf-8")
        else:
            g_event = 0                         # Received second event, KEY
            g_key = msg['data'].decode("utf-8")
            print("%s %s" % (g_command, g_key)) # Print to stdout
            g_command = g_key = ""              

def set_shutdown(debug):
    """
    set_shutdown Begin the shutdown process of the program
    
    :debug: Debug print information
    """
    global g_shutdown
    g_shutdown = 1
    
    if g_debug: 
        sys.stderr.write("SHUTDOWN: %s %d\n" % (debug, g_shutdown))

def signal_handler(signum, frame): 
    """
    Handles signals thrown by system 
    :signum: Signal ID
    :frame: Frame of execution interrupted
    """
    # If we catch one of these signals, begin shutdown of program
    match signum:
        case signal.SIGINT:
            set_shutdown("SIGINT")
        case signal.SIGTERM:
            set_shutdown("SIGTERM")
        case signal.SIGQUIT:
            set_shutdown("SIGQUIT")
        case signal.SIGHUP:
            set_shutdown("SIGHUP")
        case signal.SIGPIPE:
            set_shutdown("SIGPIPE")

def main():
    global g_shutdown, g_debug, g_event
    g_shutdown = 0 # Determines if we should shut down program 
    g_debug = 0    # Enables debug prints to console
    g_event = 0    # Keeps track of our command and key as they come in 
    
    parser = argparse.ArgumentParser(description="Wait for a key to appear in Redis", add_help=False)
    args = getargs(parser)
    
    # Connect to Redis server
    rserver = redis_connect(args.serverip, args.serverport, args.database)

    # Throw out the signal fishing net
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGPIPE, signal_handler)

    subscribe_key = "*" # Subscribe to all topics
    
    pubsub = rserver.pubsub()

    pubsub.psubscribe(**{subscribe_key: event_handler})  # Tie the subscriber to the event handler
    thread = pubsub.run_in_thread(sleep_time=0.1)        # Spin up a thread to run the subscriber

    while not g_shutdown: # Main thread will poll for a shutdown
        continue

    thread.stop() # Merc the thread

if __name__ == "__main__":
    main()