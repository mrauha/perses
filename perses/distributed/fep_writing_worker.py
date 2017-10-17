import perses.distributed.feptasks as feptasks
import pika
from pika import spec, channel as rmq_channel
import pickle
from io import BytesIO
import logging
import os
import sys

_logger = logging.getLogger(__name__)

def write_equilibrium_callback(ch: rmq_channel, method: spec.Basic.Deliver, properties: spec.BasicProperties, body: bytes):
    """
    This function is called when there is a message to write the result of an equilibrium task.

    Parameters
    ----------
    ch : pika.channel
    method : spec.Basic.Deliver
    properties : spec.BasicProperties
    body : bytes
    """
    routing_key = method.routing_key

    ch.exchange_declare(exchange="equilibrium", exchange_type="topic")

    #we will use the routing key to define a filename
    routing_key_parts = routing_key.split(".")
    filename = "_".join(routing_key_parts) + ".h5"
    full_filepath = os.path.join(destination_directory, filename)

    input_bytes = BytesIO(initial_bytes=body)
    input_arguments = pickle.load(input_bytes)

    try:
        trajectory = input_arguments['trajectory']

    except KeyError as e:
        error_routing_key = routing_key + ".error"
        msg = "The required key %s was not present" % str(e)
        ch.basic_publish(exchange="equilibrium", routing_key=error_routing_key, body=msg.encode(encoding="utf-8"))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    feptasks.write_equilibrium_trajectory(trajectory, full_filepath)

    ch.basic_ack(delivery_tag=method.delivery_tag)

def write_nonequilibrium_callback(ch: rmq_channel, method: spec.Basic.Deliver, properties: spec.BasicProperties, body: bytes):
    """
    This function is called when there is a message to write the result of a nonequilibrium task.

    Parameters
    ----------
    ch : pika.channel
    method : spec.Basic.Deliver
    properties : spec.BasicProperties
    body : bytes
    """
    routing_key = method.routing_key

    ch.exchange_declare(exchange="nonequilibrium", exchange_type="topic")

    input_bytes = BytesIO(initial_bytes=body)
    input_arguments = pickle.load(input_bytes)

    try:
        trajectory = input_arguments['trajectory']
        cumulative_work = input_arguments['cumulative_work']
        cycle_count = input_arguments['cycle_count']

    except KeyError as e:
        error_routing_key = routing_key + ".error"
        msg = "The required key %s was not present" % str(e)
        ch.basic_publish(exchange="nonequilibrium", routing_key=error_routing_key, body=msg.encode(encoding="utf-8"))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    routing_key_parts = routing_key.split(".")
    filename = "_".join(routing_key_parts)
    traj_filename = filename + "%d.h5" % cycle_count
    cum_work_filename = filename + "cum_work_%d.npy" % cycle_count
    full_filepath_traj = os.path.join(destination_directory, traj_filename)
    full_filepath_cum_work = os.path.join(destination_directory, cum_work_filename)

    feptasks.write_nonequilibrium_trajectory(trajectory, cumulative_work, full_filepath_traj, full_filepath_cum_work)

    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__=="__main__":
    rabbitmq_location = "localhost"

    #get the destination directory from the arguments
    destination_directory = sys.argv[1]

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_location))
    channel = connection.channel()

    #we'll be listening to these exchanges
    exchanges = ['equilibrium', 'nonequilibrium']

    #on these queues:
    queues = ['write_equilibrium', 'write_nonequilbrium']

    for exchange in exchanges:
        channel.exchange_declare(exchange=exchange, exchange_type="topic")

    for queue in queues:
        channel.queue_declare(queue=queue)

    #bind the queues to the appropriate topics:
    channel.queue_bind(queue="write_equilibrium", exchange="equilibrium", routing_key="*.*.equilibrium.trajectory")
    channel.queue_bind(queue="write_nonequilibrium", exchange="nonequilibrium", routing_key="*.*.nonequilibrium.trajectory")

    #register the callbacks
    channel.basic_consume(write_equilibrium_callback, queue="write_equilibrium")
    channel.basic_consume(write_nonequilibrium_callback, queue="write_nonequilibrium")

    #set QoS to prefetch count 1--this prevents multiple messages from being delivered to the same worker
    #while leaving other workers idle
    channel.basic_qos(prefetch_count=1)

    _logger.info("Beginning consume task...")

    channel.start_consuming()