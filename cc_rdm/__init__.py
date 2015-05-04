"""

Package to handle transferring data into Archivematica via Globus then transfer resulting
artifacts to distribution/archival storage.

"""

from globusonline.transfer.api_client import get_access_token
from globusonline.transfer.api_client import TransferAPIClient, Transfer
import bagit
import os

class Configuration:
	"""
	Stores information needed to complete an operation.
	
	"""
	
	def __init__(self, src_endpoint = None, dip_endpoint = None, aip_endpoint = None,
				 proc_dir = None, ingest_dir = None, aip_dir = None, dip_dir = None, 
				 globus_user = None, globus_pass = None, a_rest_url = None,
				 a_user = None, a_api_key = None, needs_preservation = False):
		"""
		Create new configuration.
		
		Parameters:
			src_endpoint - Globus endpoint where data to ingest from is stored 
			dip_endpoint - Globus endpoint where DIP will be stored
			aip_endpoint - Globus endpoint where AIP will be stored
			proc_dir - Directory where files are stored when moving
			ingest_dir - Directory Archivematica operations are started from
			aip_dir - Directory AIPs will be copied from
			dip_dir - Directory DIPs will be copied from
			globus_user - Globus username to transfer under
			globus_pass - Globus password (need to see if alternate method exists)
			needs_preservatiopn - Is AIP creation and storage necessary
		
		"""
		self.src_endpoint = src_endpoint
		self.dip_endpoint = dip_endpoint
		self.aip_endpoint = aip_endpoint
		self.proc_dir = proc_dir
		self.ingest_dir = ingest_dir
		self.aip_dir = aip_dir
		self.dip_dir = dip_dir
		self.globus_user = globus_user
		self.globus_pass = globus_pass
		self.needs_preservation = needs_preservation
		self.a_rest_url = a_rest_url
		self.a_user = a_user
		self.a_api_key = a_api_key
		self.api = None
		
	def get_api(self):
		"""
		Get the Globus TransferAPIClient associated with this configuration
	
		Returns:
			A globusonline.transfer.api_client.TransferAPIClient
			
		Raises:
			ConfigException if configuration is not sufficient to create the API.
			Exceptions passed from Globus transfer API		
		"""
		if self.api == None:
			if self.globus_user == None:
				raise ConfigException("No Globus username set.")
			elif self.globus_pass == None:
				raise ConfigException("No Globus password set.")
			else:
				 auth_tok = get_access_token(username = self.globus_user, 
											 password = self.globus_pass)
				 self.api = TransferAPIClient(username = auth_tok.username, 
											  goauth=auth_tok.token) 
		return self.api
		
	def is_complete(self):
		"""
		Check if the configuration is complete enough to attempt an operation
		
		Returns:
			True if required endpoints and Globus information is complete. False otherwise
		"""
		res = True

		if self.globus_user == None or self.globus_pass == None:
			res = False
		
		return res
		
class BagOperation:
	"""
	Handles bag processing
	"""
	def __init__(self, dir = '.', metadata = {}, bag = None):
		"""
		Create a new BagOperation
		
		Parameters:
			dir - Directory bag is in
			metadata - Dictionary containing the metadata for this bag
			bag - The bag this operation works on
		"""
		self.dir = dir
		self.metadata = {}
		self.bag = bag
		
	def make_bag(self):
		"""
		Create a new bag from the given directory.
		
		Returns:
			The bag that is created.
		"""
		self.bag = bagit.make_bag(self.dir, self.metadata)
		return self.bag
		
	def is_valid(self):
		"""
		Validate bag.
		
		Returns:
			True if bag is valid, false otherwise
		"""
		if self.bag == None:
			return False
		else:
			return self.bag.is_valid()
		
		
class TransferOperation:
	"""
	Handles a transfer using Globus
	"""
	def __init__(self, config = None, src_dir = '/', dest_dir = '/', file_list = []):
		self.config = config
		self.src_dir = src_dir
		self.dest_dir = dest_dir
		self.file_list = file_list
		self.go_task_id = None # Globus transfer ID - used internally
		
	def _get_api(self):
		return self.config.get_api()
	
	def start_transfer(self, src = None, dest = None):
		"""
		Starts a transfer
		
		"""
		
		if src == None:
			 src = self.config.src_endpoint
		
		if dest == None:
			dest = self.config.proc_endpoint
		
		if self.file_list == None or len(self.file_list) == 0:
			raise TransferException("No files specified for transfer")
		
		api = self._get_api()
		
		# activate endpoints
		_, _, data = api.endpoint_autoactivate(src)
		_, _, data = api.endpoint_autoactivate(dest)
		
		# create transfer and populate files
		_, _, data = api.transfer_submission_id()
		submission_id = data["value"]
		t = Transfer(submission_id, src, dest)
		for file in self.file_list:
			# TODO use path manipulation tools to do this, cleaner
			t.add_item(os.path.join(self.src_dir, file), os.path.join(self.dest_dir, file))
		
		# transfer
		_, _, data = api.transfer(t)
		self.go_task_id = data['task_id']
		
	def transfer_status(self):
		"""
		Reports status of this transfer.
		
		Raises:
			TransferException if no transfer is set.
			Passed through Globus API exceptions.
		"""
		if self.go_task_id == None:
			raise TransferException("No task information available.")
		
		api = self._get_api()
		_, _, data = api.task(self.go_task_id, fields="status")
		
		return data["status"]
		
		
class IngestOperation:
	"""
	Handles an ingestion to Archivematica
	"""
	def __init__(self, config = None, bagname = None):
		"""
		Create a new IngestOperation
		
		Parameters:	
			config - Configuration object that contains info on directories for Archivematica ingestion
			bagname - Name of the bag to be ingested.
		"""
		self.config = config
		self.bagname = bagname
		
	def can_ingest(self):
		"""
		Check there is sufficient information to proceed with the ingestion.
		
		Returns:
			True if there is enough information to ingest
		"""
		res = True
		if self.config == None:
			res = False
		elif self.config.proc_dir == None:
			res = False
		elif self.config.ingest_dir == None:
			res = False
		elif self.config.dip_dir == None:
			res = False
		elif self.config.needs_preservation and self.config.aip_dir == None:
			res = False
		elif self.bagname == None:
			res = False
			
		return res
		
	def ingest(self, copy_file = False):
		"""
		Trigger Archivematica to ingest the bag specified by bagname
		"""
		
		if self.can_ingest() == False:
			raise IngestException("Not enough information to ingest bag.")
			
		# Copy the file to the A ingest directory if needed
		if copy_file:
			os.rename(os.path.join(self.config.proc_dir, self.bagname), 
					  os.path.join(self.config.ingest_dir, self.bagname))
					  
		
		
		
		
class IngestException(Exception):
	"""
	Raised when an ingest operation fails
	"""	
	pass
		
class ConfigException(Exception):
	"""
	Raised when configuration information is missing or incorrect.
	"""
	pass
	
class TransferException(Exception):
	"""
	Raised when transfer fails.
	"""
	pass