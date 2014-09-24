import logging, os, sys

def init_logger(sid,type,date):
    log = logging.getLogger('')
    log.setLevel(logging.DEBUG)

    if not os.path.isdir('/home/ec2-user/log/DB_Feed/' + date + '/'):
        os.makedirs('/home/ec2-user/log/DB_Feed/' + date + '/')

    fh = logging.FileHandler('/home/ec2-user/log/DB_Feed/' + date + '/excitedigitalmedia_' + type + '_click_logs_' + sid + '_' + date + '.log')
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)

    fmrt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    fh.setFormatter(fmrt)
    ch.setFormatter(fmrt)
    log.addFilter(ch)
    log.addHandler(fh)

    return log
