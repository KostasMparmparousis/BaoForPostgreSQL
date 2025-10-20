# BAO Experiment Reproduction Guide

This document provides the specific commands required to replicate the experiments presented in our paper using the BAO optimizer.

### Workflow Reminder

For every command, remember the BAO workflow:
1.  **Terminal 1:** Start the BAO server (`cd bao_server && python3 main.py`).
2.  **Terminal 2:** **Modify the hardcoded model path** in the relevant Python script (`train.py` or `test_run.py`).
3.  **Terminal 2:** Run the command listed below.

-   All paths should be relative to the `optimizers/BaoForPostgreSQL/` directory.
-   `<path/to/huggingface/models/>` refers to the directory where you downloaded the pretrained model checkpoints.

---

## (E1) End-to-End Performance & Value Model Fidelity

### Training

1.  **Start Server:** In terminal 1, `cd bao_server && python3 main.py`.
2.  **Edit `train.py`:** Set `model_dir` on line 39 to `../../experiments/experiment1/job/train/models/bao/`.
3.  **Run Training:** In terminal 2:
    ```bash
    python3 train.py --query_dir ../../experiments/experiment1/job/train/ \
                    --output_file ../../experiments/experiment1/job/train/results/bao_training_log.txt
    ```

### Testing

1.  **Start Server:** In terminal 1, `cd bao_server && python3 main.py`.
2.  **Edit `test_run.py`:** Set `FINAL_MODEL_DIR` on line 139 to `<path/to/huggingface/models/>/E1/bao_job/`.
3.  **Run Testing:** In terminal 2:
    ```bash
    python3 test_run.py \
        ../../experiments/experiment1/job/test/ \
        ../../experiments/experiment1/job/test/results/bao_test_output.txt \
        imdbload
    ```

---

## (E2) Sensitivity & Execution Stability

### Training
```bash
# Add your training steps for E2 here (Start Server, Edit File, Run Command)
```

### Testing
```bash
# Add your testing steps for E2 here (Start Server, Edit File, Run Command)
```

---

## (E3) Learning Trajectory & Convergence

### Training
```bash
# Add your training steps for E3 here (Start Server, Edit File, Run Command)
```

### Testing
```bash
# Add your testing steps for E3 here (Start Server, Edit File, Run Command)
```