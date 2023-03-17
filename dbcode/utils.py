def deprecated(fn):
    def function(*args):
        function_name = fn.__name__
        print(f'[warning] ({function_name}) this function is deprecated!')
        return fn(*args)
    return function
