import pandas as pd
import time
import multiprocessing as mp
from Engine import WordleEngine
import random

# --- CONFIGURATION ---
TEST_HARD_MODE = True  # Set to True for Hard Mode testing, False for Standard Mode
SAMPLE_SIZE = 200
NUM_CORES = mp.cpu_count() - 1


def worker_task(word_chunk, log_queue):
    """Processes a chunk and sends live updates to the queue."""
    # Engine loaded once per core for efficiency
    engine = WordleEngine()

    for target in word_chunk:
        target = target.lower().strip()
        engine.reset()
        turns = 0

        while True:
            turns += 1
            strat, _ = engine.get_suggestions(is_hard_mode=TEST_HARD_MODE)

            if not strat:
                log_queue.put((target, -1))  # Signal failure
                break

            guess = strat[0]["word"]
            if guess == target:
                log_queue.put((target, turns))  # Signal success
                break

            pattern = engine.calculate_pattern(guess, target)
            engine.update_state(guess, pattern)

            if turns >= 10:
                log_queue.put((target, 11))  # Signal timeout
                break


def listener_task(log_queue, total_expected):
    count = 0
    results = []
    morgue = []  # Detailed tracking for Turn 7+ cases
    # Expanded distribution to track Turn 7 explicitly
    distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, "fail": 0}
    start_time = time.time()

    while count < total_expected:
        target, turns = log_queue.get()
        count += 1

        if 0 < turns <= 6:
            distribution[turns] += 1
            results.append(turns)
        elif turns >= 7:
            # Record exactly how many turns it took in the morgue
            morgue.append(f"{target.upper()} ({turns} turns)")
            if turns == 7:
                distribution[7] += 1
                results.append(7)
            else:
                distribution["fail"] += 1
        else:
            distribution["fail"] += 1
            morgue.append(f"{target.upper()} (COLLAPSED)")

        if count % 10 == 0 or count == total_expected:
            avg = sum(results) / len(results) if results else 0
            print(
                f"[PROFILING: {count:04d}/{total_expected}] | Avg: {avg:.3f} | Last: {target.upper()}"
            )

    total_time = time.time() - start_time
    # Survival is strictly 6 turns or fewer
    success_count = sum(1 for r in results if r <= 6)
    failure_count = len(morgue)

    print("\n" + "═" * 50)
    print("FORENSIC ARCHITECTURE REPORT")
    print("═" * 50)
    print(f"STATUS:         MISSION COMPLETE")
    print(f"THROUGHPUT:     {total_expected / total_time:.2f} words/sec")
    print(
        f"EFFICIENCY:     {sum(results)/len(results) if results else 0:.4f} Avg Turns"
    )
    print(f"SURVIVAL RATE:  {((success_count / total_expected) * 100):.2f}%")
    print("═" * 50)

    print("SOLUTION DENSITY DISTRIBUTION:")
    for t in range(1, 8):
        count_val = distribution[t]
        percent = (count_val / total_expected) * 100
        bar = "█" * int(percent / 2)
        tag = (
            "[SURPASSES PROOF]"
            if t <= 5
            else "[CRITICAL STRESS]" if t == 6 else "[MISSION FAILURE]"
        )
        print(f" Turn {t}: {percent:5.1f}% | {bar} ({count_val}) {tag}")

    if morgue:
        print("\nTHE MORGUE (Words exceeding Turn 6):")
        print(f" Total Identified: {failure_count}")
        # Print words in columns
        for i in range(0, len(morgue), 4):
            print("   " + " | ".join(morgue[i : i + 4]))

    print("\nARCHITECTURAL VERDICT:")
    if failure_count == 0:
        print(" > PROOF VALIDATED: TRUE SOLVER. 100% Efficiency within 6 turns.")
    else:
        print(
            f" > PROOF ADJUSTED: OPTIMIZER. {failure_count} cases exceeded the limit."
        )
    print("═" * 50)


def run_live_stress_test():
    try:
        # 1. Load the full solution set verbatim
        solutions_df = pd.read_csv("valid_solutions.csv")
        all_solutions = solutions_df.iloc[:, 0].dropna().tolist()

        # 2. Textbook Random Sampling
        # This ensures we aren't just testing 'A' words or 'first' words
        if len(all_solutions) > SAMPLE_SIZE:
            print(
                f"Randomly selecting {SAMPLE_SIZE} targets from pool of {len(all_solutions)}..."
            )
            test_pool = random.sample(all_solutions, SAMPLE_SIZE)
        else:
            print(
                f"Sample size exceeds pool. Testing all {len(all_solutions)} words."
            )
            test_pool = all_solutions
            random.shuffle(test_pool)  # Shuffle anyway for non-linear testing

    except Exception as e:
        print(f"Data Error: {e}")
        return

    # Set up Multiprocessing structures[cite: 2, 3]
    manager = mp.Manager()
    log_queue = manager.Queue()

    # 3. Chunk the randomized pool
    chunk_size = len(test_pool) // NUM_CORES
    chunks = [
        test_pool[i : i + chunk_size] for i in range(0, len(test_pool), chunk_size)
    ]

    actual_total = sum(len(c) for c in chunks)

    print(f"STARTING RANDOMIZED LIVE STRESS TEST ({NUM_CORES} CORES)")
    print(
        f"MODAL ANALYSIS: {'HARD MODE' if TEST_HARD_MODE else 'NORMAL MODE (SOLVER)'}"
    )
    print("-" * 50)

    # Start the listener thread/process for real-time printing
    listener = mp.Process(target=listener_task, args=(log_queue, actual_total))
    listener.start()

    # Start workers
    processes = []
    for chunk in chunks:
        p = mp.Process(target=worker_task, args=(chunk, log_queue))
        p.start()
        processes.append(p)

    # Wait for everything to finish
    for p in processes:
        p.join()
    listener.join()


if __name__ == "__main__":
    run_live_stress_test()
