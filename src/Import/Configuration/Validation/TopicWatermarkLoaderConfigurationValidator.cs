﻿using System.Diagnostics;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import.Configuration.Validation;

/// <summary>
/// Validator for <see cref="TopicWatermarkLoaderConfiguration"/>.
/// </summary>
public class TopicWatermarkLoaderConfigurationValidator : IValidateOptions<TopicWatermarkLoaderConfiguration>
{
    /// <summary>
    /// Validates <see cref="TopicWatermarkLoaderConfiguration"/>.
    /// </summary>
    public ValidateOptionsResult Validate(string name, TopicWatermarkLoaderConfiguration options)
    {
        Debug.Assert(name is not null);
        Debug.Assert(options is not null);

        if (options.AdminClientTimeout == TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("Timeout should not be zero.");
        }

        return ValidateOptionsResult.Success;
    }
}
