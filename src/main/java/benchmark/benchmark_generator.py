import random
import string
import os

# --- Operation types ---
VOLATILE_OPS = ["INSERT", "UPDATE", "DELETE", "INCREMENT", "DECREMENT"]
STABLE_OPS = ["GET", "VISUALISE"]
CONTROL_OPS = ["COMMIT", "ROLLBACK"]
ALL_OPS = VOLATILE_OPS + STABLE_OPS + CONTROL_OPS
columns = ["X", "Y", "Z"]


table_pool = ["K1", "K2"]
next_table_index = 3
commit_counter = 1


def random_string(length=5):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def maybe_add_new_table(probability=0.2):
    global next_table_index
    if random.random() < probability:
        new_table = f"K{next_table_index}"
        table_pool.append(new_table)
        next_table_index += 1

def choose_table():
    maybe_add_new_table()
    weights = [1 / (i + 1) for i in range(len(table_pool))]
    total = sum(weights)
    probs = [w / total for w in weights]
    return random.choices(table_pool, weights=probs, k=1)[0]


def generate_insert(): return f"INSERT {choose_table()} {random.choice(columns)} {random.randint(100, 1000)}"
def generate_update(): return f"UPDATE {choose_table()} {random.choice(columns)} {random.randint(1000, 2000)}"
def generate_increment(): return f"INCREMENT {choose_table()} {random.choice(columns)} {random.randint(1000, 2000)}"
def generate_decrement(): return f"DECREMENT {choose_table()} {random.choice(columns)} {random.randint(1000, 2000)}"
def generate_delete(): return f"DELETE {choose_table()} {random.choice(columns)} 0"
def generate_get(): return f"GET {choose_table()}"
def generate_visualise(): return f"VISUALISE {choose_table()}"

def generate_commit(commits):
    global commit_counter
    commit_id = f"C{commit_counter}"
    commit_counter += 1
    commits.append(commit_id)
    return f"COMMIT {commit_id}"

def generate_rollback(commits):
    if not commits:
        return f"ROLLBACK {random_string()}"
    return f"ROLLBACK {random.choice(commits)}"

def generate_query(op, commits):
    if op == "COMMIT":
        return generate_commit(commits)
    elif op == "ROLLBACK":
        return generate_rollback(commits)
    else:
        return {
            "INSERT": generate_insert,
            "UPDATE": generate_update,
            "DECREMENT": generate_decrement,
            "INCREMENT": generate_increment,
            "DELETE": generate_delete,
            "GET": generate_get,
            "VISUALISE": generate_visualise
        }[op]()


def generate_taskset(num_queries):
    task = []
    commits = []
    for _ in range(num_queries):
        op = random.choice(ALL_OPS)
        task.append(generate_query(op, commits))
    return task

def write_tasksets(n, queries_per_task, output_dir="tasksets"):
    os.makedirs(output_dir, exist_ok=True)
    for i in range(1, n + 1):
        filename = os.path.join(output_dir, f"T{i}")
        with open(filename, "w") as f:
            taskset = generate_taskset(queries_per_task)
            f.write("\n".join(taskset))
    print(f"Generated {n} tasksets in '{output_dir}/'")
    print("Final table pool:", table_pool)

if __name__ == "__main__":
    write_tasksets(n=5, queries_per_task=5000)
