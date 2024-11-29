class Messages:
    SUCCESS = {
        "S1": "Database successfully initialized",
        "S2": "User successfully added",
        "S3": "DVD successfully added",
        "S4": "User successfully removed",
        "S5": "DVD successfully removed",
        "S6": "DVD successfully checked out",
        "S7": "DVD successfully returned and rated",
        "S8": "Bye!",
    }

    ERROR = {
        "E1": "Title length should range from 1 to 100 characters",
        "E2": "Author length should range from 1 to 50 characters",
        "E3": "DVD ({title}, {director}) already exists",
        "E4": "Username length should range from 1 to 50 characters",
        "E5": "DVD {d_id} does not exist",
        "E6": "Cannot delete a DVD that is currently borrowed",
        "E7": "User {u_id} does not exist",
        "E8": "Cannot delete a user with borrowed DVDs",
        "E9": "Cannot check out a DVD that is out of stock",
        "E10": "User {u_id} exceeded the maximum borrowing limit",
        "E11": "Rating should range from 1 to 5 integer value",
        "E12": "Cannot return and rate a DVD that is not currently borrowed for this user",
        "E13": "({name}, {age}) already exists",
        "E14": "Age should be a positive integer",
        "E15": "User cannot borrow the same DVD simultaneously",
        "E16": "Cannot find any matching results",
    }

    @staticmethod
    def get_message(code, **kwargs):
        if code in Messages.SUCCESS:
            return Messages.SUCCESS[code]
        elif code in Messages.ERROR:
            return Messages.ERROR[code].format(**kwargs)
        else:
            return "Unknown message code"
