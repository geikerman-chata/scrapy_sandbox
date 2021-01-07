from fp.fp import FreeProxy
import time


def fetch_proxies():
    proxy_list = []
    attempts = 0
    while len(proxy_list) < 10 and attempts < 25:
        tic = time.time()
        proxy = FreeProxy(country_id=['US', 'CA', 'GB', 'FR', 'JP'], rand=True).get()
        if proxy:
            proxy_truncated = proxy.replace('http://','')
            if proxy_truncated not in proxy_list:
                proxy_list.append(proxy_truncated)
                toc = time.time()
                print('Proxy Added: {} on attempt {}, time elapsed: {}'.format(proxy_truncated, attempts, round(toc-tic, 3)))
        attempts += 1

    return proxy_list


if __name__ =='__main__':
    proxy_list = fetch_proxies()
    with open('proxies.txt', 'w') as writer:
        for proxy in proxy_list:
            writer.write("%s\n" % proxy)

