Coordinator
===========

1.  Coordinator of events.  Wait for all event to happen, trigger action when event happen.
2.  Score Keeper (TBD)
3.  Mutex (TBD)

##Flow - Coordinating order using election.
```
				Client			MainService			Coordinator			DependencySubSystem
				---neworder-------->
									---neworder---------------------------->

Timeout or pending()

								    ---alert()-------->

order is still pending
				---get_status()----->
				<--PENDING-----------								   

				
2mins later dependency sub system update order.
													   <-------------send()---
								    <-------notify()---
								    finish_order()

				---get_status()----->
				<--SUCCESS-----------

```

##Functions

```  
  note: dependency: the fields must be consistant across all the system.

  /**
  setup a new alert.  some event might be send before this method is call, so always keep events.  
  match_fields: the fields in the event that will be counted. eg. sender,tx_id
  count_threshold: the count reach before firing alert_url
  alert_url: the url to alert.  system will post to alert_url with { count:, evts: [],}
  **/
  alert(evtname, count_threshold=1, alert_url=null, match_fields)
  POST /alert?evtname=&count_threshold=&alert_url=  

  example: POST /alert?evtname=neworder&alert_Url=http://localhost/time_order_handler
  {
  	seller: 'ABC',  	
  	tx_id: 1234,
  	status: 'SUCCESS'
  }
  
  send(evtname,sender, params, ttl=1h)
  POST /events?evtname=&sender=&ttl=1h
  body = {cid:, seller:, tx_id:, status:,}
  
```  
