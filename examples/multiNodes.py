import argparse
import asyncio
import logging
import time
from kademlia.network import Server
from random import choice, sample
import psutil

def kill_process_by_port(port):
    for conn in psutil.net_connections(kind='inet'):
        if conn.laddr.port == port:
            try:
                process = psutil.Process(conn.pid)
                process.terminate()
                print(f"Process using port {port} terminated.")
            except psutil.NoSuchProcess:
                print(f"No process using port {port} found.")
            return

def main(num_nodes, num_sets, num_gets):
    logging.basicConfig(level=logging.INFO, filename='network.log', filemode='w')

    async def run():
        total_set_time = 0
        total_get_time = 0        
        # Create the first node
        node1 = Server()
        await node1.listen(8468)
        logging.info("Node 1 created and listening on port 8468")

        # Bootstrap the node (in this case, itself since it's the first node)
        await node1.bootstrap([("localhost", 8468)])

        # Create other nodes and bootstrap them to a random existing node
        nodes = [node1]
        for i in range(2, num_nodes + 1):
            node = Server()
            await node.listen(8468 + i)

            retries = 5
            for attempt in range(retries):
                bootstrap_node = choice(nodes)
                bootstrap_port = 8468 + nodes.index(bootstrap_node)                

                try:
                    res = await node.bootstrap([("localhost", bootstrap_port)])
                    if not res:
                        logging.error(f"Attempt {attempt + 1} to connect node {i} returned an empty result.")
                    else:
                        logging.info(f"res: %s", str(res))
                        logging.info(f"Node {i} successfully connected on attempt {attempt + 1}")
                        break
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1} to connect node {i} failed: {e}")
                    if attempt == retries - 1:
                        logging.error(f"Node {i} failed to connect after {retries} attempts")
                    else:
                        await kill_process_by_port(8468 + i)
                        node.stop()
                        await asyncio.sleep(2**attempt)  # Exponential backoff
            nodes.append(node)

        # Set values on random nodes
        for i in range(1, num_sets + 1):
            node = choice(nodes)
            start_time = time.time()
            await node.set(f"key-{i}", f"value-{i}")
            elapsed_time = time.time() - start_time
            total_set_time += elapsed_time
            logging.info(f"Time taken to set value {i}: {elapsed_time} seconds")

        # Retrieve the values from random nodes
        for node in sample(nodes, num_gets):
            for i in range(1, num_sets + 1):
                start_time = time.time()
                value = await node.get(f"key-{i}")
                elapsed_time = time.time() - start_time
                total_get_time += elapsed_time
                logging.info(f"Time taken to get value {i}: {elapsed_time} seconds, Value retrieved: {value}")
        
        average_set_time = total_set_time / (num_sets)
        average_get_time = total_get_time / (num_sets * num_gets)
        logging.info(f"Average time for a set operation: {average_set_time} seconds")
        logging.info(f"Average time for a get operation: {average_get_time} seconds")
        # try:
        #     await asyncio.sleep(3600)  # Keep the nodes running for an hour
        # finally:
        for node in nodes:
            node.stop()

    # Run the asynchronous function
    asyncio.run(run())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a Kademlia network')
    parser.add_argument('--nodes', type=int, required=True, help='Number of nodes')
    parser.add_argument('--sets', type=int, required=True, help='Number of values to be set')
    parser.add_argument('--gets', type=int, required=True, help='Number of gets to be performed')
    args = parser.parse_args()

    main(args.nodes, args.sets, args.gets)
