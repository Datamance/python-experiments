import uuid
import random
from gevent import subprocess, queue, spawn
from pprint import pprint

# 0) Set up some stuff we're going to use later
LETTER_SWATCH = tuple([chr(n) for n in xrange(65, 91)] + [chr(n) for n in xrange(97, 123)])
LETTERS_LEN = 52

NUM_WORKERS = 4

input_queue = queue.Queue()
output_queue = queue.Queue()


def worker():
    worker = subprocess.Popen(['python', '-u', 'worker.py'],
                              shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
    while True:
        worker.stdin.write(input_queue.get())
        output_queue.put(worker.stdout.readline())


# 2) Now, let's test some pool mechanics.
#    First, we have an initial set of experiments - for the sake of ease
#    of description, the "VGDL" is just some junk string, and the value
#    is the score of the experiment.

experiments = {
    'abcxdyrl': None,
    'oiuhdnuiadsa': None
}


def get_new_experiments(current_experiments):
    """Gets rid of poor performers and mutates good ones slightly.

    Meant to mimic Pedro's experiment.
    """
    new_experiments = {k: v for k, v in current_experiments.iteritems() if v is None}
    for experiment, score in current_experiments.iteritems():
        if score > 0.7:
            new_experiments[experiment] = score
            for _ in range(3):  # Generate up to three new experiments
                # Replace one letter.
                random_letter = LETTER_SWATCH[random.randint(0, LETTERS_LEN - 1)]
                random_idx = random.randint(0, len(experiment) - 1)
                exp_components = [char for char in experiment]
                if exp_components[random_idx] != random_letter:
                    exp_components[random_idx] = random_letter
                    new_experiments[''.join(exp_components)] = None

    return new_experiments


if __name__ == '__main__':
    for x in range(NUM_WORKERS):
        print 'spawning worker'
        spawn(worker)

    while not any([score >= 0.9 for score in experiments.values()]):
        # Evenly chunk out the available experiments, so that each worker gets the
        # same amount of work.
        experiments = get_new_experiments(experiments)
        vgdl_list = [vgdl for (vgdl, score) in experiments.iteritems() if score is None]
        # Now that we've chunked, shove each list to a worker.
        for sub_list in [vgdl_list[i::NUM_WORKERS] for i in xrange(NUM_WORKERS)]:
            if not sub_list:
                break

            input_queue.put(' '.join(sub_list) + '\n')

        results = output_queue.get()
        for unserialized in results.split():
            exp, string_score = unserialized.split(':')
            score = float(string_score)
            experiments[exp] = score

        print 'EXPERIMENTS: '
        pprint({exp: score for exp, score in experiments.iteritems() if score is not None})
