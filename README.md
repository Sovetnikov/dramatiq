# dramatiq

Fork of dramatiq with useful things:

* Support of priority queues with redis broker. Tasks with lower priority values will be executed first.
* Default task priority can be configured with "dramatiq_actor_default_priority" environment variable, because default priority in dramatiq is 0.
* get_result method by default propagates exceptions if task failed (exceptions stored in result backend).
* Group has new method "get_any_result" that returns group results as fast as they ready independent of order and without waiting for all results.
* Task priority can be set at run time with send_with_options(options=dict(priority=X)).
* Workers can be restarted by request from middleware or task by raising exception RestartWorker
* MaxTasksPerChild middleware, like in celery for restarting workers after number of tasks processed 

## License

dramatiq is licensed under the LGPL.  Please see [COPYING] and
[COPYING.LESSER] for licensing details.


[COPYING.LESSER]: https://github.com/Bogdanp/dramatiq/blob/master/COPYING.LESSER
[COPYING]: https://github.com/Bogdanp/dramatiq/blob/master/COPYING
[RabbitMQ]: https://www.rabbitmq.com/
[Redis]: https://redis.io
[user guide]: https://dramatiq.io/guide.html
