import re


class S3NamingHelper:

    def validate_part(self, value: str, allow_prefix: bool = True) -> tuple:
        """ Checks if value is valid in an S3 key

        Args:
            value (str): The S3 path part to validate
            allow_prefix (bool, Optional): Indicates whether this path part 
                includes multiple "folders" to it

        Returns:
            A tuple, of which the first index being False means it is invalid
                and a second index will contain the message as to why
        """
        if not allow_prefix and '/' in value:
            return tuple([False, 'prefix dissalowed'])
        if len(value) < 1:
            return tuple([False, 'path parts must be 1 or more characters'])

        vals = value.split('/')
        for part in vals:
            # get rid of those leading / trailing slashes
            if len(part) > 0:
                for c in part:
                    if c not in self._safe_chars():
                        return tuple([False, f'{c} is not an allowed character.'])

        return tuple([True, None])

    def validate_bucket_name(self, bucket_name: str) -> bool:
        """ Checks if value is valid as an S3 bucket name

        Args:
            bucket_name (str): The S3 bucket name to validate

        Returns:
            A bool, which should always be True if no error was thrown

        Raises:
            ValueError: If the bucket name was invalid, with a message as to why
        """
        result, message = self._validate_bucket_name(bucket_name)
        if result:
            return result
        else:
            raise ValueError(message)

    def _validate_bucket_name(self, bucket_name: str) -> tuple:
        ''' INTENT: checks for a valid bucket name
            ARGS: 
                - bucket_name (str) the bucket name to validate
            RETURNS: tuple validation and reason value
        '''
        # must be between 3-63 chars
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            return tuple([False, 'bucket name must be between 3 and 63 chars'])

        # lower case chars, numbers, periods, dashes
        elif not bucket_name.islower():
            return tuple([False, 'bucket name cannot contain upper case characters'])

        elif not bool(re.match(r"^[a-z0-9\-\.]*$", bucket_name)):
            return tuple([False, 'bucket name can only contain lower case chars, numbers, dashes and periods'])

        # cannot end with dash
        elif bucket_name.endswith('-'):
            return tuple([False, 'bucket name cannot end with a dash'])

        # cannot consecutive periods
        elif '..' in bucket_name:
            return (False, 'bucket name cannot include double periods')

        # dashes next to periods
        elif '.-' in bucket_name or '-.' in bucket_name:
            return (False, 'bucket name cannot have dashes next to periods')

        # char or number after period
        elif bool(re.search(r"\.[^0-9a-z]*", bucket_name)):
            return tuple([False, 'bucket name must have only a letter or a number after a period'])

        # char or number at start
        elif not (bucket_name[0].isalpha() or bucket_name[0].isnumeric()):
            return tuple([False, 'bucket name must start with a number or letter'])
        else:
            return tuple([True, None])

    def validate_s3_path(self, path: str) -> tuple:
        """ Checks if value is valid as a complete S3 path/URI

        Args:
            path (str): The S3 URI to validate

        Returns:
            A tuple, of which the first index being False means it is invalid
                and a second index will contain the message as to why
        """
        if path[:5] != 's3://':
            return tuple([False, 'bucket path must have arn prefix (s3://)'])

        path_parts = path[5:].split('/')

        bucket_validity = self._validate_bucket_name(path_parts[0])

        if not bucket_validity[0]:
            return tuple([False, bucket_validity[1]])

        for part in path_parts[1:]:
            part_validity = self.validate_part(part)
            if not part_validity[0]:
                return tuple([False, part_validity[1]])

        return tuple([True, path])

    def _safe_chars(self) -> list:
        """ Returns a list of all characters safe for use in S3 """
        safe = list(range(ord('a'), ord('z')+1))
        safe += list(range(ord('A'), ord('Z')+1))
        safe = [chr(x) for x in safe]
        safe += [str(x) for x in range(0, 10)]
        safe += ['!', '-', '_', '.', '*', '(', ')', '=']
        return safe
