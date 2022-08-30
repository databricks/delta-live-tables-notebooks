import sys
import create_tables

def lambda_handler(event, context):
    op = event['operation']

    if op == 'create':
        create_tables.main({},{})
    elif op == 'populate':
        import populate_tables
        populate_tables.main({},{})
    elif op == 'modify':
        import modify_tables
        modify_tables.main({},{})


if __name__ == '__main__':
    op = sys.argv[1]

    lambda_handler({'operation':op},{})