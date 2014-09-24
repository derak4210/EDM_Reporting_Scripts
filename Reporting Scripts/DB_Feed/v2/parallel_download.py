import pycurl

def download_logs(queue):
    
    num_conn = 100

    # Check args
    assert queue, "no URLs given"
    num_urls = len(queue)
    num_conn = min(num_conn, num_urls)
    assert 1 <= num_conn <= 10000, "invalid number of concurrent connections"
    print "PycURL %s (compiled against 0x%x)" % (pycurl.version, pycurl.COMPILE_LIBCURL_VERSION_NUM)
    print "----- Getting", num_urls, "URLs using", num_conn, "connections -----"


    # Pre-allocate a list of curl objects
    m = pycurl.CurlMulti()
    m.handles = []
    for i in range(num_conn):
        c = pycurl.Curl()
        c.fp = None
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.MAXREDIRS, 5)
        c.setopt(pycurl.CONNECTTIMEOUT, 30)
        c.setopt(pycurl.TIMEOUT, 30000000)
        c.setopt(pycurl.NOSIGNAL, 1)
        c.setopt(pycurl.ENCODING, '')
        m.handles.append(c)


    # Main loop
    freelist = m.handles[:]
    num_processed = 0
    while num_processed < num_urls:
        # If there is an url to process and a free curl object, add to multi stack
        while queue and freelist:
            url, filename = queue.pop(0)
            c = freelist.pop()
            c.fp = open(filename, "wb")
            c.setopt(pycurl.URL, url)
            c.setopt(pycurl.WRITEDATA, c.fp)
            m.add_handle(c)
            # store some info
            c.filename = filename
            c.url = url
        # Run the internal curl state machine for the multi stack
        while 1:
            ret, num_handles = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        # Check for curl objects which have terminated, and add them to the freelist
        while 1:
            num_q, ok_list, err_list = m.info_read()
            for c in ok_list:
                c.fp.close()
                c.fp = None
                m.remove_handle(c)
                print "Success:", c.filename, c.url, c.getinfo(pycurl.EFFECTIVE_URL)
                freelist.append(c)
            for c, errno, errmsg in err_list:
                c.fp.close()
                c.fp = None
                m.remove_handle(c)
                print "Failed: ", c.filename, c.url, errno, errmsg
                freelist.append(c)
            num_processed = num_processed + len(ok_list) + len(err_list)
            if num_q == 0:
                break
        # Currently no more I/O is pending, could do something in the meantime
        # (display a progress bar, etc.).
        # We just call select() to sleep until some more data is available.
        m.select(1.0)


    # Cleanup
    for c in m.handles:
        if c.fp is not None:
            c.fp.close()
            c.fp = None
        c.close()
    m.close()
