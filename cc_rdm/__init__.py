"""

Package to handle transferring data into Archivematica via Globus then transfer resulting
artifacts to distribution/archival storage.

"""

from globusonline.transfer.api_client import get_access_token
from globusonline.transfer.api_client import TransferAPIClient, Transfer
import bagit
import os
import requests

class Configuration:
	"""
	Stores information needed to complete an operation.
	
	"""
	
	def __init__(self, globus_user = None, globus_pass = None, a_api_host = None,
				 a_user = None, a_api_key = None, needs_preservation = False):
		"""
		Create new configuration.
		
		Parameters:
			globus_user - Globus username to transfer under
			globus_pass - Globus password (need to see if alternate method exists)
			a_user - Archivematica API username
			a_api_key - Archivematica API key
			a_api_host - Archivematica hostname to run API queries against
			needs_preservatiopn - Is AIP creation and storage necessary
		
		"""
		self.globus_user = globus_user
		self.globus_pass = globus_pass
		self.needs_preservation = needs_preservation
		self.a_api_host = a_api_host
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
		
	def is_complete_globus(self):
		"""
		Check if the configuration is complete enough to attempt a transfer operation
		
		Returns:
			True if required Globus information is complete. False otherwise
		"""
		res = True

		if self.globus_user == None or self.globus_pass == None:
			res = False
		
		return res
		
	def is_complete_arc(self):
		"""
		Check if the configuration is complete enough to attempt an ingestion operation
		Returns:
			True if required Archivematica information is present. False otherwise.
		"""
		
		res = True
		if self.a_user == None or self.a_api_host == None or self.a_api_key == None:
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
	
	def add_file(self, filename):
		"""
		Adds a file to the list of files to transfer
		"""
		self.file_list.append(filename)
	
	def start_transfer(self, src, dest):
		"""
		Starts a transfer
		
		"""
				
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
	def __init__(self, config = None, bagname = None, type = 'unzipped bag'):
		"""
		Create a new IngestOperation
		
		Parameters:	
			config - Configuration object that contains info on directories for Archivematica ingestion
			bagname - Name of the bag to be ingested.
			type - transfer type. Defaults to 'unzipped bag'.
		"""
		self.config = config
		self.bagname = bagname
		self.type = type
		
	def can_ingest(self):
		"""
		Check there is sufficient information to proceed with the ingestion.
		
		Returns:
			True if there is enough information to ingest
		"""
		res = True
		if self.config == None:
			res = False
		elif self.bagname == None:
			res = False
			
		return res
		
	def get_unapproved(self):
		params = {'username': self.config.a_user, 'api_key': self.config.a_api_key}
		api_path = '/api/transfer/unapproved'
		r = requests.get(self.config.a_api_host + api_path, params=params)
		return r.json()
		
	def ingest(self, bagname = None, type = None):
		"""
		Trigger Archivematica to ingest the bag specified by bagname
		"""
		
		if bagname != None:
			self.bagname = bagname
			
		if type != None:
			self.type = type
		
		if self.can_ingest() == False:
			raise IngestException("Not enough information to ingest bag.")
		
		params = {'username': self.config.a_user, 'api_key': self.config.a_api_key, 
				  'directory': self.bagname, 'type': self.type}
		api_path = '/api/transfer/approve'
		r = requests.get(self.config.a_api_host + api_path, params = params)
		return r
		

		
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