import sys, os, tempfile, shutil, re, base64, time, ssl, getpass, socket

from urllib.request import build_opener, install_opener, Request, urlopen
from urllib.request import HTTPHandler, HTTPSHandler, HTTPCookieProcessor
from urllib.error import HTTPError, URLError
from http.cookiejar import MozillaCookieJar

abort = False

class BulkDownloader:
    def __init__(self, downloadFolder, files=[], insecure=False):
        # Username/Password
        self.username = "hussainmahmood"
        self.password = "Hybridme$123"
        # List of files to download
        self.downloadFolder = downloadFolder
        self.files = files

        # Local stash of cookies so we don't always have to ask
        self.cookie_jar_path = os.path.join(self.downloadFolder, ".bulk_download_cookiejar.txt")
        self.cookie_jar = None

        self.asf_urs4 = { 'url': 'https://urs.earthdata.nasa.gov/oauth/authorize',
                 'client': 'BO_n7nTIlMljdvU6kRRB3g',
                 'redir': 'https://auth.asf.alaska.edu/login'}

        # Make sure we can write it our current directory
        if os.access(downloadFolder, os.W_OK) is False:
            print (f"WARNING: Cannot write to current path! Check permissions for {downloadFolder}")
            raise Exception("Errored out")

        # For SSL
        self.context = {}
        if insecure:
          try:
              ctx = ssl.create_default_context()
              ctx.check_hostname = False
              ctx.verify_mode = ssl.CERT_NONE
              self.context['context'] = ctx
          except AttributeError:
              # Python 2.6 won't complain about SSL Validation
              pass

        # Make sure cookie_jar is good to go!
        self.get_cookie()

         # summary
        self.total_bytes = 0
        self.total_time = 0
        self.cnt = 0
        self.success = []
        self.failed = []
        self.skipped = []


    # Get and validate a cookie
    def get_cookie(self):
       if os.path.isfile(self.cookie_jar_path):
          self.cookie_jar = MozillaCookieJar()
          self.cookie_jar.load(self.cookie_jar_path)

          # make sure cookie is still valid
          if self.check_cookie():
             print("Reusing previous cookie jar.")
             return True
          else:
             print("Could not validate old cookie Jar")

       # We don't have a valid cookie, prompt user or creds
       print ("No existing URS cookie found, please enter Earthdata username & password:")
       print ("(Credentials will not be stored, saved or logged anywhere)")

       # Keep trying 'till user gets the right U:P
       while self.check_cookie() is False:
          self.get_new_cookie()

       return True

    # Validate cookie before we begin
    def check_cookie(self):

       if self.cookie_jar is None:
          print (f"Cookiejar is bunk: {self.cookie_jar}")
          return False

       # File we know is valid, used to validate cookie
       file_check = 'https://urs.earthdata.nasa.gov/profile'

       # Apply custom Redirect Hanlder
       opener = build_opener(HTTPCookieProcessor(self.cookie_jar), HTTPHandler(), HTTPSHandler(**self.context))
       install_opener(opener)

       # Attempt a HEAD request
       request = Request(file_check)
       request.get_method = lambda : 'HEAD'
       try:
          print (f"Attempting to download {file_check}")
          response = urlopen(request, timeout=30)
          resp_code = response.getcode()
          # Make sure we're logged in
          if not self.check_cookie_is_logged_in(self.cookie_jar):
             return False

          # Save cookiejar
          self.cookie_jar.save(self.cookie_jar_path)

       except HTTPError:
          # If we ge this error, again, it likely means the user has not agreed to current EULA
          print ("\nIMPORTANT: ")
          print ("Your user appears to lack permissions to download data from the ASF Datapool.")
          print ("\n\nNew users: you must first log into Vertex and accept the EULA. In addition, your Study Area must be set at Earthdata https://urs.earthdata.nasa.gov")
          raise Exception("Errored out")

       # This return codes indicate the USER has not been approved to download the data
       if resp_code in (300, 301, 302, 303):
          try:
             redir_url = response.info().getheader('Location')
          except AttributeError:
             redir_url = response.getheader('Location')

          #Funky Test env:
          if ("vertex-retired.daac.asf.alaska.edu" in redir_url and "test" in self.asf_urs4['redir']):
             print ("Cough, cough. It's dusty in this test env!")
             return True

          print (f"Redirect ({resp_code}) occured, invalid cookie value!")
          return False

       # These are successes!
       if resp_code in (200, 307):
          return True

       return False

    def get_new_cookie(self):
       # Start by prompting user to input their credentials

       new_username = input("Username: ")
       new_password = getpass.getpass(prompt="Password (will not be displayed): ")

       # Build URS4 Cookie request
       auth_cookie_url = self.asf_urs4['url'] + '?client_id=' + self.asf_urs4['client'] + '&redirect_uri=' + self.asf_urs4['redir'] + '&response_type=code&state='
       user_pass = base64.b64encode (bytes(new_username+":"+new_password, "utf-8"))
       user_pass = user_pass.decode("utf-8")
       print(user_pass)

       # Authenticate against URS, grab all the cookies
       self.cookie_jar = MozillaCookieJar()
       opener = build_opener(HTTPCookieProcessor(self.cookie_jar), HTTPHandler(), HTTPSHandler(**self.context))
       request = Request(auth_cookie_url, headers={"Authorization": "Basic {0}".format(user_pass)})

       # Watch out cookie rejection!
       try:
          response = opener.open(request)
       except HTTPError as e:
          if "WWW-Authenticate" in e.headers and "Please enter your Earthdata Login credentials" in e.headers["WWW-Authenticate"]:
             print ("Username and Password combo was not successful. Please try again.")
             return False
          else:
             # If an error happens here, the user most likely has not confirmed EULA.
             print ("\nIMPORTANT: There was an error obtaining a download cookie!")
             print ("Your user appears to lack permission to download data from the ASF Datapool.")
             print ("\n\nNew users: you must first log into Vertex and accept the EULA. In addition, your Study Area must be set at Earthdata https://urs.earthdata.nasa.gov")
             raise Exception("Errored out")
       except URLError as e:
          print ("\nIMPORTANT: There was a problem communicating with URS, unable to obtain cookie. ")
          print ("Try cookie generation later.")
          raise Exception("Errored out")

       # Did we get a cookie?
       if self.check_cookie_is_logged_in(self.cookie_jar):
          #COOKIE SUCCESS!
          self.cookie_jar.save(self.cookie_jar_path)
          return True

       # if we aren't successful generating the cookie, nothing will work. Stop here!
       print ("WARNING: Could not generate new cookie! Cannot proceed. Please try Username and Password again.")
       print (f"Response was {response.getcode()}.")
       print ("\n\nNew users: you must first log into Vertex and accept the EULA. In addition, your Study Area must be set at Earthdata https://urs.earthdata.nasa.gov")
       raise Exception("Errored out")

    # make sure we're logged into URS
    def check_cookie_is_logged_in(self, cj):
       for cookie in cj:
          if cookie.name == 'urs_user_already_logged':
              # Only get this cookie if we logged in successfully!
              return True

       return False


    # Download the file
    def download_file_with_cookiejar(self, url, file_count, total, recursion=False):
       # see if we've already download this file and if it is that it is the correct size
       download_file = os.path.join(self.downloadFolder, os.path.basename(url).split('?')[0])
       if os.path.isfile(download_file):
          try:
             request = Request(url)
             request.get_method = lambda : 'HEAD'
             response = urlopen(request, timeout=30)
             remote_size = self.get_total_size(response)
             # Check that we were able to derive a size.
             if remote_size:
                 local_size = os.path.getsize(download_file)
                 if remote_size < (local_size+(local_size*.01)) and remote_size > (local_size-(local_size*.01)):
                     print (f"Download file {download_file} exists! \nSkipping download of {url}. ")
                     return None,None
                 #partial file size wasn't full file size, lets blow away the chunk and start again
                 print (f"Found {download_file} but it wasn't fully downloaded. Removing file and downloading again.")
                 os.remove(download_file)

          except ssl.CertificateError as e:
             print (f"ERROR: {e}")
             print ("Could not validate SSL Cert. You may be able to overcome this using the --insecure flag")
             return False,None

          except HTTPError as e:
             if e.code == 401:
                 print ("IMPORTANT: Your user may not have permission to download this type of data!")
             else:
                 print (f"Unknown Error, Could not get file HEAD: {e}")

          except URLError as e:
             print (f"URL Error (from HEAD): {e.reason}, {url}")
             if "ssl.c" in f"{e.reason}":
                 print ("IMPORTANT: Remote location may not be accepting your SSL configuration. This is a terminal error.")
             return False,None

       # attempt https connection
       try:
          request = Request(url)
          response = urlopen(request, timeout=30)

          # Watch for redirect
          if response.geturl() != url:

             # See if we were redirect BACK to URS for re-auth.
             if 'https://urs.earthdata.nasa.gov/oauth/authorize' in response.geturl():

                 if recursion:
                     print ("Entering seemingly endless auth loop. Aborting. ")
                     return False, None

                 # make this easier. If there is no app_type=401, add it
                 new_auth_url = response.geturl()
                 if "app_type" not in new_auth_url:
                     new_auth_url += "&app_type=401"

                 print (f"While attempting to download {url}....")
                 print (f"Need to obtain new cookie from {new_auth_url}")
                 old_cookies = [cookie.name for cookie in self.cookie_jar]
                 opener = build_opener(HTTPCookieProcessor(self.cookie_jar), HTTPHandler(), HTTPSHandler(**self.context))
                 request = Request(new_auth_url)
                 try:
                     response = opener.open(request)
                     for cookie in self.cookie_jar:
                         if cookie.name not in old_cookies:
                              print (f"Saved new cookie: {cookie.name}")

                              # A little hack to save session cookies
                              if cookie.discard:
                                   cookie.expires = int(time.time()) + 60*60*24*30
                                   print ("Saving session Cookie that should have been discarded!")

                     self.cookie_jar.save(self.cookie_jar_path, ignore_discard=True, ignore_expires=True)
                 except HTTPError as e:
                     print (f"HTTP Error: {e.code}, {url}")
                     return False,None

                 # Okay, now we have more cookies! Lets try again, recursively!
                 print ("Attempting download again with new cookies!")
                 return self.download_file_with_cookiejar(url, file_count, total, recursion=True)

             print (f"'Temporary' Redirect download @ Remote archive:\n{response.geturl()}")

          # seems to be working
          print (f"({file_count}/{total}) Downloading {url}")

          # Open our local file for writing and build status bar
          tf = tempfile.NamedTemporaryFile(mode='w+b', delete=False, dir=self.downloadFolder)
          self.chunk_read(response, tf, report_hook=self.chunk_report)

          # Reset download status
          sys.stdout.write('\n')

          tempfile_name = tf.name
          tf.close()

       #handle errors
       except HTTPError as e:
          print (f"HTTP Error: {e.code}, {url}")

          if e.code == 401:
             print ("IMPORTANT: Your user does not have permission to download this type of data!")

          if e.code == 403:
             print ("Got a 403 Error trying to download this file.  ")
             print ("You MAY need to log in this app and agree to a EULA. ")

          return False,None

       except URLError as e:
          print (f"URL Error (from GET): {e}, {e.reason}, {url}")
          if "ssl.c" in f"{e.reason}":
              print ("IMPORTANT: Remote location may not be accepting your SSL configuration. This is a terminal error.")
          return False,None

       except socket.timeout as e:
           print (f"timeout requesting: {url}; {e}")
           return False,None

       except ssl.CertificateError as e:
          print (f"ERROR: {e}")
          print ("Could not validate SSL Cert. You may be able to overcome this using the --insecure flag")
          return False,None

       # Return the file size
       shutil.copy(tempfile_name, download_file)
       os.remove(tempfile_name)
       file_size = self.get_total_size(response)
       actual_size = os.path.getsize(download_file)
       if file_size is None:
           # We were unable to calculate file size.
           file_size = actual_size
       return actual_size,file_size

    def get_redirect_url_from_error(self, error):
       find_redirect = re.compile(r"id=\"redir_link\"\s+href=\"(\S+)\"")
       print (f"error file was: {error}")
       redirect_url = find_redirect.search(error)
       if redirect_url:
          print(f"Found: {redirect_url.group(0)}")
          return (redirect_url.group(0))

       return None


    #  chunk_report taken from http://stackoverflow.com/questions/2028517/python-urllib2-progress-hook
    def chunk_report(self, bytes_so_far, file_size):
       if file_size is not None:
           percent = float(bytes_so_far) / file_size
           percent = round(percent*100, 2)
           sys.stdout.write("Downloaded %d of %d bytes (%0.2f%%)\r" %
               (bytes_so_far, file_size, percent))
       else:
           # We couldn't figure out the size.
           sys.stdout.write("Downloaded %d of unknown Size\r" % (bytes_so_far))

    #  chunk_read modified from http://stackoverflow.com/questions/2028517/python-urllib2-progress-hook
    def chunk_read(self, response, local_file, chunk_size=8192, report_hook=None):
       file_size = self.get_total_size(response)
       bytes_so_far = 0

       while 1:
          try:
             chunk = response.read(chunk_size)
          except:
             sys.stdout.write("\nThere was an error reading data. \n")
             break

          try:
             local_file.write(chunk)
          except TypeError:
             local_file.write(chunk.decode(local_file.encoding))
          bytes_so_far += len(chunk)

          if not chunk:
             break

          if report_hook:
             report_hook(bytes_so_far, file_size)

       return bytes_so_far

    def get_total_size(self, response):
       try:
          file_size = response.info().getheader('Content-Length').strip()
       except AttributeError:
          try:
             file_size = response.getheader('Content-Length').strip()
          except AttributeError:
             print ("Problem getting size")
             return None

       return int(file_size)

    # Download all the files in the list
    def download_files(self):
        for file_name in self.files:

            # make sure we haven't ctrl+c'd or some other abort trap
            if abort == True:
              raise SystemExit

            # download counter
            self.cnt += 1

            # set a timer
            start = time.time()

            # run download
            size,total_size = self.download_file_with_cookiejar(file_name, self.cnt, len(self.files))

            # calculte rate
            end = time.time()

            # stats:
            if size is None:
                self.skipped.append(file_name)
            # Check to see that the download didn't error and is the correct size
            elif size is not False and (total_size < (size+(size*.01)) and total_size > (size-(size*.01))):
                # Download was good!
                elapsed = end - start
                elapsed = 1.0 if elapsed < 1 else elapsed
                rate = (size/1024**2)/elapsed

                print (f"Downloaded {size}b in {elapsed:.2f}secs, Average Rate: {rate:.2f}MB/sec")

                # add up metrics
                self.total_bytes = self.total_bytes + size
                self.total_time = self.total_time + elapsed
                self.success.append( {'file':file_name, 'size':size } )

            else:
                print (f"There was a problem downloading {file_name}")
                self.failed.append(file_name)

    def print_summary(self):
        # Print summary:
        print ("\n\nDownload Summary ")
        print ("--------------------------------------------------------------------------------")
        print (f"  Successes: {len(self.success)} files, {self.total_bytes} bytes ")
        for success_file in self.success:
           print (f"           - {success_file['file']}  {(success_file['size']/1024.0**2):.2f}MB")
        if len(self.failed) > 0:
           print (f"  Failures: {len(self.failed)} files")
           for failed_file in self.failed:
              print (f"          - {failed_file}")
        if len(self.skipped) > 0:
           print (f"  Skipped: {len(self.skipped)} files")
           for skipped_file in self.skipped:
              print (f"          - {skipped_file}")
        if len(self.success) > 0:
           print (f"  Average Rate: {((self.total_bytes/1024.0**2)/self.total_time):.2f}MB/sec")
        print ("--------------------------------------------------------------------------------")
