def get_sftp():
    print("sftp 작업을 시작합니다")

def regist(name, sex, *args):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션들: {args}')

def log_decorator(task_func):
    def make_log(*args):
        print("작업을 시작합니다")
        task_func(*args)
        print("작업을 종료합니다")
    return make_log