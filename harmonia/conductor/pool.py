from concurrent.futures import ThreadPoolExecutor, wait


def run_graph(graph):
    with ThreadPoolExecutor() as executor:
        futures = []
        for node in graph:
            futures.append(executor.submit(node))
        wait(futures)
