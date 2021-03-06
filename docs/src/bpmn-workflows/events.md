# Events

Currently supported elements:

![workflow](/bpmn-workflows/none-start-event.png)
![workflow](/bpmn-workflows/none-end-event.png)

## Start Events

A workflow must have exactly one none start event. The event is triggered when the workflow is started via API and in consequence a token spawns at the event.

XML representation:

```
<bpmn:startEvent id="order-placed" name="Order Placed" />
```

## End Events

A workflow can have one or more none end events.
When a token arrives at an end event, then the it is consumed.
If it is the last token, then the entire workflow instance ends.

XML representation:

```
<bpmn:endEvent id="order-delivered" name="Order Delivered" />
```

Note that an activity without outgoing sequence flow has the same semantics as a none end event.
After the task is completed, the token is consumed and the workflow instance may end.
