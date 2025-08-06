CURRENT_SH_DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
python "$CURRENT_SH_DIR/workflow_setup.py"
python "$CURRENT_SH_DIR/DB_setup.py"

