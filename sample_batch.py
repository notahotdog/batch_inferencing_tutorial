'''
    [Working Implementation]

    Used to simulate processing queries for summarization in batches

    - Used to process the queries from multiple api calls into a single queue 
    - Queries are then selected in batches and processed
'''
from urllib.parse import urlencode
import asyncio

SUMMARIZATION_BATCH = [] 
BATCH_MODEL_LOOP_RUNNING = False
N_QUERIES_TO_PROCESS = 1 #TODO - set this number to decide on how many queries to process at 1 time

async def process_batches_loop():
    '''
        Driver code for running/processing the items in the queue
    '''

    print("\n -- Processing batch... -- \n")
 
    global SUMMARIZATION_BATCH, BATCH_MODEL_LOOP_RUNNING, N_QUERIES_TO_PROCESS

    #Driver code for running the queue 
    while BATCH_MODEL_LOOP_RUNNING: #When the batch_loop is initialized

        await asyncio.sleep(0.0000000000000001) #Not sure why but my code works now
      
        if not SUMMARIZATION_BATCH: # if nothing in the queue, continue - len(S_B) = 0 == False
            continue

        n_items_processing = N_QUERIES_TO_PROCESS 

        batch = SUMMARIZATION_BATCH[:n_items_processing]
        SUMMARIZATION_BATCH= SUMMARIZATION_BATCH[n_items_processing:]

        queries = [query for (query,future) in batch] # Extract all the queries from batch

        summaries = await get_summarization_result(queries) # Return a dictionary with key value pair 
        print("\n[BATCH results processed]\n")

        #Update the query status
        for query, future in batch:
           summarization_results = summaries.get(query) #Extract using the dictionary key 
           future.set_result(summarization_results)
           print(f"Future set for: {query} | Future state: {future}")

# Change this to something else
async def get_summarization_result(queries): 
    #TODO: Replace with batch processing logic - simple logic provided

    print("\nProcessing new batch ")

    result_dic = {}

    queries = list(set(queries)) # make the queries unique - incase of repeated spam request

    for query in queries:
        mock_response = query.split(" ")[-1]
        result_dic[query] = mock_response 

    #Get the queries and return it as a dictionary 
    print(f"Results processed: {result_dic}")

    return  result_dic

'''
    2. Initializes queries into futures and puts it into a queue to be processed
'''
async def get_summarization(query):
    '''Returns ( Result from your batch processor)'''

    #Initialise the queue if not running
    global BATCH_MODEL_LOOP_RUNNING

    if not BATCH_MODEL_LOOP_RUNNING:
        print(f"[Starting batch queue]\n")
        BATCH_MODEL_LOOP_RUNNING = True
        asyncio.create_task(process_batches_loop()) #Creates a batch queue process

    print(f"Create task for query: {query}")

    loop = asyncio.get_event_loop() # References the python environment
    future= loop.create_future()
    # print("Event loop state:", loop)
    # print("\nFuture created for:",query, "\nfuture state:", future, "\n")

    SUMMARIZATION_BATCH.append((query,future)) #appends the query and future into the queue

    print("Awaiting future for ", query)
    await future

    infer_result = future.result()

    print(f"Summarization completed for query: {query} -- result: {infer_result}")

    # return future.result()
    return infer_result


'''
    Collates all the different queries
'''
async def main():

    print(f"[Starting model inference]\n")
    model_tasks = []
    
    #Sample Queries
    queries = [
        '''What is the avg age of an octopus''',
        '''What is the avg age of an elephant''', 
        '''What is the avg age of a human''', 
        '''What is the avg age of a tortoise''',
        '''What is the avg age of a buffalo''',
        '''What is the avg age of a person'''
        ]
    
    for query in queries:
        model_tasks.append(get_summarization(query)) #Create task for query

    batch_response = await asyncio.gather(*model_tasks) # Waits until every task is completed
    
    print("\n\nMain completed")
    for i in batch_response: # Print responses
        print(i)
    
    #Stop batch_process
    global BATCH_MODEL_LOOP_RUNNING
    BATCH_MODEL_LOOP_RUNNING = False  

if __name__ == "__main__":
    loop = asyncio.new_event_loop() # Because it's an async function we need to run it using the asyncio event loop
    loop.run_until_complete(main()) # Needs to be run this way to execute async def instance
    loop.close()
    print("Loop closed:", loop)

