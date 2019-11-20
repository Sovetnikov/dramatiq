# dramatiq

Fork of dramatiq with useful things:

* support of priority queues with redis broker
* actors exception propagation in get_result (exceptions stored in result backend)
* group has new method get_any_result that returns group results as fast as they ready independent of order
* task priority can be set at run time with send_with_options(options=dict(priority=X))
* default actors priority can be configured with "dramatiq_actor_default_priority" environment variable, because default priority 0 is highest priority and that makes hard to make other tasks high priority.
* workers can be restarted by request from middleware or task
* MaxTasksPerChild middleware like in celery for restarting workers after number of tasks processed 

## License

dramatiq is licensed under the LGPL.  Please see [COPYING] and
[COPYING.LESSER] for licensing details.


[COPYING.LESSER]: https://github.com/Bogdanp/dramatiq/blob/master/COPYING.LESSER
[COPYING]: https://github.com/Bogdanp/dramatiq/blob/master/COPYING
[RabbitMQ]: https://www.rabbitmq.com/
[Redis]: https://redis.io
[user guide]: https://dramatiq.io/guide.html
