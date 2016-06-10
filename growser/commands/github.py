from growser.cmdr import Command


class UpdateFromGitHubAPI(Command):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.name)


class BatchUpdateFromGitHubAPI(Command):
    def __init__(self, limit: int, batch_size: int,
                 rating_window: int=90, task_window: int=30):
        """Update local repository data using the GitHub API.

        For example, to update 1,250 repositories in batches of 100 based on the
        most number of ratings in the prior 180 days that have not already been
        updated in the prior 45 days::

            command = BatchUpdateFromGitHubAPI(1250, 100, 180, 45)

        :param limit: Total number of repositories to update.
        :param batch_size: Number of API requests to wait for before updating
                           our local data.
        :param rating_window: Prioritize repositories based on the number of
                              events within this window of days.
        :param task_window: Don't include repositories that have already been
                            updated within this number of days.

        ..note:: Will be deprecated once event listeners have been implemented.
        """
        self.limit = limit
        self.batch_size = batch_size
        self.rating_window = rating_window
        self.task_window = task_window
