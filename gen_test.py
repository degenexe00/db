if __name__ == "__main__":
    # Define the number of insertions
    num_insertions = 14



    # Open a file to write
    with open('test_case.txt', 'w') as file:
        for i in range(1, num_insertions + 1):
            # Write the insert command to the file
            file.write(f'insert {i} user{i} user{i}@example.com\n')

    print("File 'insert_commands.txt' created with insert commands.")
