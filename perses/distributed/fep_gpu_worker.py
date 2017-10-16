import perses.distributed.feptasks as feptasks
import pika
from pika import spec, channel
from typing import Union
import pickle
from io import BytesIO


def minimize_callback(ch: channel, method: spec.Basic.Deliver, properties: spec.BasicProperties, body: bytes):
    """
    The callback function that handles tasks placed in the minimize queue. The arguments to this function have a standard
    form based on the AMQP client library pika. The data to be processed is in the body. This method acknowledges the completion
    of its task.
    """
    pass



def equilibrium_callback(ch: channel, method: spec.Basic.Deliver, properties: spec.BasicProperties, body: bytes):
    """
    The method that is called to process a request to run some equilibrium dynamics. The body needs to contain:
    sampler_state, thermodynamic_state, mc_move, topology, n_iterations, n_cycles (this is the number of times to run the
    equilibrium-nonequilbrium pairs), and optionally may contain atom_indices_to_save, which indicates that not all atoms
    should be saved in the trajectory, as well as run_nonequilibrium. When called, this method will also follow up by calling
    the nonequilibrium switching task, if requested. It will also call itself again if n_cycles > 0, and decrement n_cycles.

    Parameters
    ----------
    ch : pika.channel
    method : spec.Basic.Deliver
    properties : spec.BasicProperties
    body : bytes
    """
    #declare the exchanges through which we will send our results:
    ch.exchange_declare(exchange='equilibrium', exchange_type="topic")
    ch.exchange_declare(exchange='nonequilibrium', exchange_type='topic')
    ch.exchange_declare(exchange='utilities', exchange_type='topic')

    #get the routing key, since we are listening to all requests for equilibrium tasks:
    routing_key = method.routing_key

    #the routing key that we are listening to is *.*.equilibrium.resume
    #the first wildcard is a calculation name, the second is the lambda state (0 or 1)
    #this is used to direct tasks to the appropriate write and log queues
    routing_key_parts = routing_key.split(".")
    calculation_name = routing_key_parts[0]
    lambda_value = routing_key_parts[1]

    equilibrium_prefix_key = ".".join([calculation_name, lambda_value, "equilibrium"])
    nonequilibrium_prefix_key = ".".join([calculation_name, lambda_value, "nonequilibrium"])

    #first, we need to unpack the contents of the body:
    input_data_bytesio = BytesIO(initial_bytes=body)
    input_arguments = pickle.load(input_data_bytesio)

    try:
        thermodynamic_state = input_arguments['thermodynamic_state']
        sampler_state = input_arguments['sampler_state']
        mc_move = input_arguments['mc_move']
        topology = input_arguments['topology']
        n_iterations = input_arguments['n_iterations']
        cycle_count = input_arguments['cycle_count']

    except KeyError as e:
        return #fill in a call back to the error queue

    #now check if the optional arguments are present. If so, unpack them:
    if 'atom_indices_to_save' in input_arguments.keys():
        atom_indices_to_save = input_arguments['atom_indices_to_save']
    else:
        atom_indices_to_save = None

    n_cycles = input_arguments['n_cycles'] if 'n_cycles' in input_arguments.keys() else 0

    run_nonequilibrium = input_arguments['run_nonequilbrium'] if 'run_nonequilibrium' in input_arguments.keys() else False

    #run the actual task
    sampler_state_new, trajectory, reduced_potential_final_frame = feptasks.run_equilibrium(sampler_state,
                                                                                            thermodynamic_state,
                                                                                            mc_move, topology,
                                                                                            n_iterations,
                                                                                            atom_indices_to_save=atom_indices_to_save)
    #increment cycle count:
    cycle_count += 1
    input_arguments['sampler_state'] = sampler_state_new
    input_arguments['cycle_count'] = cycle_count
    input_arguments['n_cycles'] = n_cycles - 1

    #serialize to bytes
    resume_task_input = BytesIO()
    pickle.dump(input_arguments, resume_task_input)

    #prepare the results:
    #These results will go to the write queue
    write_routing_key = ".".join([equilibrium_prefix_key, "trajectory"])
    traj_results = dict()
    traj_results['trajectory'] = trajectory
    traj_results['cycle_count'] = cycle_count
    traj_results_output = BytesIO()
    pickle.dump(traj_results, traj_results_output)

    #publish the results to the trajectory writing topic:
    ch.basic_publish(exchange='equilibrium', routing_key=write_routing_key, body=traj_results_output.getvalue())

    #We also want to publish the energy and some basic result status to the logging task:
    energy_routing_key = ".".join([equilibrium_prefix_key, "energy_output"])
    energy_results = dict()
    energy_results['reduced_potential'] = reduced_potential_final_frame
    energy_results['cycle_count'] = cycle_count
    energy_results['sampler_state'] = sampler_state_new #include this so that the endpoint perturbation can be calculated
    energy_output = BytesIO()
    pickle.dump(energy_results, energy_output)

    #publish this to the energy output topic:
    ch.basic_publish(exchange="equilibrium", routing_key=energy_routing_key, body=energy_output.getvalue())

    #if the number of equilibrium cycles requested is greater than zero, we should send another task to run another
    #equilibrium cycle, and decrement the counter
    if n_cycles > 0:
        #Publish to resume topic
        resume_routing_key = ".".join([equilibrium_prefix_key, "resume"])
        ch.basic_publish(exchange="equilibrium", routing_key=resume_routing_key, body=resume_task_input.getvalue())

    #finally, if the user requested nonequilibrium, send a message to the nonequilibrium task queue to run that.
    #at this point, the input data structure needs to have included the appropriate mcmcmove for nonequilibrium
    #switching--we cannot generate it here. If it's not there, the nonequilibrium switching task will return an error
    if run_nonequilibrium:
        nonequilibrium_routing_key = ".".join([nonequilibrium_prefix_key, "run"])

        #the information needed to run this will also be in the resume task input.
        ch.basic_publish(exchange="nonequilibrium", routing_key=nonequilibrium_routing_key, body=resume_task_input.getvalue())

def nonequilibrium_callback(ch: channel, method: spec.Basic.Deliver, properties: spec.BasicProperties, body: bytes):
    """
    This is a callback that is called when there is a request to run nonequilibrium switching protocols. It calls the
    appropriate task, and then sorts the results into various topics for writing and logging.

    Parameters
    ----------
    ch : pika.channel
    method : spec.Basic.Deliver
    properties : spec.BasicProperties
    body : bytes
    """

    #declare the exchanges that we will use
    ch.exchange_declare(exchange='nonequilibrium', exchange_type='topic')
    ch.exchange_declare(exchange='utilities', exchange_type='topic')

    #get the routing key, since we are listening to all requests for equilibrium tasks:
    routing_key = method.routing_key

    #the routing key that we are listening to is *.*.equilibrium.resume
    #the first wildcard is a calculation name, the second is the lambda state (0 or 1)
    #this is used to direct tasks to the appropriate write and log queues
    routing_key_parts = routing_key.split(".")
    calculation_name = routing_key_parts[0]
    lambda_value = routing_key_parts[1]

    nonequilibrium_prefix_key = ".".join([calculation_name, lambda_value, "nonequilibrium"])

    input_arguments_bytes = BytesIO(initial_bytes=body)
    input_arguments = pickle.load(input_arguments_bytes)

    #unpack the input arguments:
    try:
        sampler_state = input_arguments['sampler_state']
        thermodynamic_state = input_arguments['thermodynamic_state']
        ne_mc_move = input_arguments['ne_mc_move']
        topology = input_arguments['topology']
        n_iterations = input_arguments['n_iterations']
    except KeyError as e:
        return

    if 'atom_indices_to_save' in input_arguments.keys():
        atom_indices_to_save = input_arguments['atom_indices_to_save']
    else:
        atom_indices_to_save = None

    #run the protocol
    trajectory, cumulative_work = feptasks.run_protocol(sampler_state, thermodynamic_state, ne_mc_move, topology, n_iterations, atom_indices_to_save=atom_indices_to_save)

    trajectory_routing_key = ".".join([nonequilibrium_prefix_key, "trajectory"])
    work_routing_key = ".".join([nonequilibrium_prefix_key, "work_output"])

    #send the results to the appropriate topics:

    #first, let's send results to the write task:
    write_task_bytes = BytesIO()
    write_task_input = dict()
    write_task_input['trajectory'] = trajectory
    write_task_input['cumulative_work'] = cumulative_work
    pickle.dump(write_task_input, write_task_bytes)

    #send to topic:
    ch.basic_publish(exchange='nonequilibrium', routing_key=trajectory_routing_key, body=write_task_bytes.getvalue())

    #now, let's send the cumulative work trajectory (much smaller) to the work output topic
    work_output_bytes = BytesIO()
    pickle.dump(cumulative_work, work_output_bytes)

    #send to topic:
    ch.basic_publish(exchange='nonequilibrium', routing_key=work_routing_key, body=work_output_bytes.getvalue())


def reduced_potential_callback(ch, method, properties, body):
    pass

if __name__=="__main__":
    pass