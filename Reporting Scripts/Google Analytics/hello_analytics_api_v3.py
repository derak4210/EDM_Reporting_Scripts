#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

# import the Auth Helper class
import hello_analytics_api_v3_auth

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError

def main(argv):
  # Step 1. Get an analytics service object.
  service = hello_analytics_api_v3_auth.initialize_service()

  try:
    # Step 2. Get the user's first profile ID.
    profile_id = get_first_profile_id(service)

    if profile_id:
      # Step 3. Query the Core Reporting API.
      results = get_results(service, profile_id)

      # Step 4. Output the results.
      print_results(results)

  except TypeError, error:
    # Handle errors in constructing a query.
    print ('There was an error in constructing your query : %s' % error)

  except HttpError, error:
    # Handle API errors.
    print ('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason()))

  except AccessTokenRefreshError:
    # Handle Auth errors.
    print ('The credentials have been revoked or expired, please re-run '
           'the application to re-authorize')
		   
def get_first_profile_id(service):
  # Get a list of all Google Analytics accounts for this user
  accounts = service.management().accounts().list().execute()

  if accounts.get('items'):
    # Get the first Google Analytics account
    firstAccountId = accounts.get('items')[0].get('id')

    # Get a list of all the Web Properties for the first account
    webproperties = service.management().webproperties().list(accountId=firstAccountId).execute()

    if webproperties.get('items'):
      # Get the first Web Property ID
      firstWebpropertyId = webproperties.get('items')[0].get('id')

      # Get a list of all Views (Profiles) for the first Web Property of the first Account
      profiles = service.management().profiles().list(
          accountId=firstAccountId,
          webPropertyId=firstWebpropertyId).execute()

      if profiles.get('items'):
        # return the first View (Profile) ID
        return profiles.get('items')[0].get('id')

  return None
  
def get_results(service, profile_id):
  # Use the Analytics Service Object to query the Core Reporting API
  return service.data().ga().get(
      ids='ga:' + '299451',
      start_date='2014-06-02',
      end_date='2014-06-02',
      metrics='ga:avgTimeOnPage,ga:bounceRate, ga:newUsers,ga:users,ga:avgSessionDuration, ga:pageviewsPerSession', dimensions='ga:adContent', filters='ga:sourceMedium==adlux').execute()

def print_results(results):
  # Print data nicely for the user.
  if results:
    print 'First View (Profile): %s' % results.get('profileInfo').get('profileName')
    print 'Total Sessions: %s' % results.get('rows')[0][0]

  else:
    print 'No results found'	 

def get_api_query(service, table_id):
  """Returns a query object to retrieve data from the Core Reporting API.

  Args:
    service: The service object built by the Google API Python client library.
    table_id: str The table ID form which to retrieve data.
  """

  return service.data().ga().get(
      ids=table_id,
      start_date='2014-09-01',
      end_date='2014-09-08',
      metrics='ga:users,ga:newUsers,ga:bounceRate,ga:bounces,ga:pageviews,ga:pageviewsPerSession,ga:avgTimeOnPage,ga:avgSessionDuration',
      dimensions='ga:adContent',
      sort='-ga:users',
      filters='ga:sourceMedium=~excitedigitalmedia \/ cpc')

