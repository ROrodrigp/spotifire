# 🎵 Spotifire - Your Year-Round Spotify Wrapped

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Flask](https://img.shields.io/badge/Flask-2.0+-green.svg)](https://flask.palletsprojects.com/)
[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20Athena%20%7C%20S3-orange.svg)](https://aws.amazon.com)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Real-time music analytics • ML-powered insights • Always-on Spotify Wrapped**

[🚀 Quick Start](#-quick-start) • [📊 Features](#-features) • [🏗️ Architecture](#️-architecture) • [📖 Documentation](#-documentation) • [🤝 Contributing](#-contributing)

**Spotifire** is a comprehensive music analytics platform that gives you Spotify Wrapped-style insights all year round, not just at the end of December. Track your musical evolution week by week, discover your listening patterns, and get personalized music profiles powered by machine learning.

## 🌟 Key Features

- **🔄 Real-time Analytics**: Get your music insights anytime, not just once a year
- **📊 Advanced Dashboard**: Beautiful visualizations of your listening patterns
- **🤖 AI Music Profiles**: ML-powered clustering to identify your unique music personality
- **⏰ Temporal Analysis**: See how your music taste evolves over time
- **🎯 Personalized Insights**: Deep analysis of your favorite artists, listening habits, and music discovery patterns
- **📱 Responsive Design**: Works perfectly on desktop and mobile devices

## 🏗️ Project Structure

```
spotifire/
├── 📁 app/                          # Main Flask application
│   ├── 📁 routes/                   # API and web routes
│   │   ├── auth.py                  # Spotify authentication
│   │   └── dashboard.py             # Dashboard and insights endpoints
│   ├── 📁 services/                 # Business logic services
│   │   ├── spotify.py               # Spotify API integration
│   │   ├── athena.py                # AWS Athena queries for insights
│   │   └── music_profiles.py        # ML music profiles service
│   └── 📁 config/                   # System configuration files
│       ├── 📁 systemd/              # Service definitions for data collection
│       └── 📁 logrotate/            # Log rotation configurations
├── 📁 scripts/                      # Data processing scripts
│   ├── spotify_periodic_collector.py    # 🕒 Collects listening data every 4 hours
│   ├── spotify_s3_uploader.py           # ☁️ Uploads collected data to S3
│   ├── update_history.py                # 📜 Collects historical data (likes, follows, top tracks)
│   ├── spotify_etl_job.py               # 🔄 Main ETL job for processing listening history
│   ├── etl_data_historica.py            # 🔄 ETL for historical data (likes, follows, top tracks)
│   ├── etl_artists_catalog.py           # 🎨 Processes artist catalog with advanced genre categorization
│   └── create_glue_catalog.py           # 🗃️ Creates AWS Glue Data Catalog tables
├── 📁 machine_learning/             # ML components
│   └── 📁 scripts/
│       └── generate_music_profiles.py   # 🧠 Generates user music profiles using K-means clustering
├── 📁 templates/                    # HTML templates
│   ├── 📁 auth/                     # Authentication pages
│   ├── 📁 dashboard/                # Dashboard templates
│   └── base.html                    # Base template
├── 📁 static/                       # Static assets
│   ├── 📁 css/                      # Stylesheets
│   └── 📁 js/                       # JavaScript files
├── config.py                        # Application configuration
├── run.py                          # Application entry point
├── requirements.txt                # Python dependencies
├── setup.sh                       # Environment setup script
└── run.sh                         # Production runner script
```

## 🚀 Quick Start

### 🌐 Try the Live Application

**Ready to explore your music data? No setup required!**

👉 **Visit: [https://52-203-107-89.nip.io/](https://52-203-107-89.nip.io/)**

1. **🎵 Connect Your Spotify**: Click "Connect with Spotify" and authorize the application
2. **🔑 Enter Your Credentials**: You'll need to create a Spotify app to get your Client ID and Secret:
   - Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
   - Create a new application
   - Set redirect URI to: `https://52-203-107-89.nip.io/callback`
   - Copy your Client ID and Client Secret
3. **📊 Start Exploring**: Once connected, explore your real-time music insights!
4. **⏰ Come Back Later**: Your data is collected automatically every 4 hours, so return to see your musical evolution

### 🔍 What You'll See

- **Immediate Insights**: Your recent tracks and current top artists
- **Listening Patterns**: Hour-by-hour breakdown of when you listen to music
- **Music Profile**: AI-powered analysis of your music personality
- **Trend Analysis**: Weekend vs weekday preferences, mainstream vs underground tastes

*Note: Advanced historical analytics become available after 24-48 hours of data collection.*

## 📊 Data Pipeline Architecture

### Real-time Data Collection

The system automatically collects your Spotify data using a sophisticated pipeline:

1. **🕒 Periodic Collection** (`scripts/spotify_periodic_collector.py`)
   - Runs every 4 hours via systemd timer
   - Collects recently played tracks
   - Stores data as CSV files with timestamps

2. **☁️ Cloud Upload** (`scripts/spotify_s3_uploader.py`)
   - Uploads collected data to S3
   - Runs 1 hour after data collection
   - Maintains organized folder structure by user

### ETL Processing (AWS Glue)

3. **🔄 Data Transformation** (`scripts/spotify_etl_job.py`)
   - Processes raw CSV files into structured Parquet format
   - Adds derived fields (play_hour, season, duration_minutes)
   - Handles data cleaning and deduplication
   - Creates user-partitioned datasets for efficient querying

4. **📜 Historical Data ETL** (`scripts/etl_data_historica.py`)
   - Processes liked tracks, followed artists, and top tracks
   - Converts JSON data to structured Parquet format
   - Maintains referential integrity with listening history

5. **🎨 Artist Catalog Processing** (`scripts/etl_artists_catalog.py`)
   - Advanced genre categorization using weighted scoring
   - Partitioned by popularity and genre for optimal query performance
   - Rich metadata for music analysis

### Analytics Layer

6. **🗃️ Data Catalog** (`scripts/create_glue_catalog.py`)
   - Creates AWS Glue tables for all processed data
   - Enables SQL queries via AWS Athena
   - Optimized schemas for analytics workloads

7. **🧠 Machine Learning** (`machine_learning/scripts/generate_music_profiles.py`)
   - K-means clustering to identify music personalities
   - 5 distinct profiles: Mainstream Explorer, Underground Hunter, Music Addict, Night Owl, Casual Listener
   - Uses 15+ features including listening patterns, artist diversity, and temporal preferences

## 🎯 Music Profiles (ML)

Our ML algorithm analyzes your listening behavior to categorize you into one of five music personalities:

- **🎯 Mainstream Explorer**: Loves current hits and follows trends
- **🔍 Underground Hunter**: Discovers artists before they become popular  
- **⚡ Music Addict**: Constantly listening to diverse music
- **🌙 Night Owl**: Prefers late-night listening sessions
- **🎵 Casual Listener**: Enjoys familiar background music

## 🖥️ Dashboard Features

### Real-time Insights
- **Recent Activity**: Your latest played tracks and current favorites
- **Top Artists**: Most listened artists with play counts and popularity metrics
- **Listening Patterns**: Hour-by-hour analysis of when you listen to music

### Historical Analytics  
- **Temporal Trends**: Weekday vs weekend listening preferences
- **Music Discovery**: Analysis of mainstream vs underground artists in your library
- **Evolution Tracking**: See how your taste changes over time

### Interactive Visualizations
- Dynamic charts powered by Chart.js
- Responsive design for all devices
- Real-time data refresh capabilities

## ⚙️ System Services Configuration

### Automated Data Collection

The system includes systemd services for automated operation:

**Data Collector Service:**
```bash
# Enable and start the periodic collector
sudo systemctl enable spotify-collector.timer
sudo systemctl start spotify-collector.timer

# Check service status
sudo systemctl status spotify-collector.timer
```

**S3 Uploader Service:**
```bash
# Enable and start the S3 uploader
sudo systemctl enable spotify-s3-uploader.timer
sudo systemctl start spotify-s3-uploader.timer
```

### Log Management

Automatic log rotation is configured for all services:
- Daily rotation for upload logs
- Weekly rotation for collection logs
- Compressed archive storage

## 🔧 AWS Configuration

### Required Services

1. **S3 Buckets**: Store raw and processed data
2. **AWS Glue**: Data catalog and ETL jobs
3. **AWS Athena**: SQL analytics engine
4. **IAM Roles**: Proper permissions for all services

### Setup AWS Resources

```bash
# Create Glue Data Catalog
python scripts/create_glue_catalog.py --database-name spotify_analytics

# Process artist catalog (if you have the data)
python scripts/etl_artists_catalog.py --input-file data/artist_catalog.json --bucket your-s3-bucket

# Generate music profiles
python machine_learning/scripts/generate_music_profiles.py --upload-s3
```

## 📈 Monitoring and Logs

### Log Locations

- Application logs: `gunicorn.log`
- Collection logs: `/var/log/spotify-collector.log`
- Upload logs: `/var/log/spotify-s3-uploader.log`
- ETL logs: AWS CloudWatch Logs

### Health Checks

```bash
# Check collector service
sudo journalctl -u spotify-collector.service -f

# Check upload service  
sudo journalctl -u spotify-s3-uploader.service -f

# Monitor application
tail -f gunicorn.log
```

## 🤝 Contributing

We welcome contributions! Here's how you can help:

### 🌟 Start by Trying the App

**First, experience Spotifire yourself**: Visit [https://52-203-107-89.nip.io/](https://52-203-107-89.nip.io/) to understand how it works and identify areas for improvement.

### Ways to Contribute

1. **🐛 Bug Reports**: Found something broken? Open an issue with detailed reproduction steps
2. **✨ Feature Requests**: Have an idea for new insights or visualizations? We'd love to hear it!
3. **🔧 Code Contributions**: Fork, create a feature branch, and submit a PR
4. **📖 Documentation**: Help improve our docs and examples
5. **🎨 UI/UX Improvements**: Make the dashboard even more beautiful and intuitive


## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This project is for educational and personal use only. It uses the Spotify Web API in compliance with their terms of service. Make sure to:

- Respect Spotify's API rate limits
- Only collect data for users who have explicitly authorized your application
- Follow data privacy best practices
- Comply with applicable data protection regulations

## 🙏 Acknowledgments

- [Spotify Web API](https://developer.spotify.com/documentation/web-api/) for providing access to music data
- [AWS](https://aws.amazon.com/) for cloud infrastructure services
- [Flask](https://flask.palletsprojects.com/) for the web framework
- [Chart.js](https://www.chartjs.org/) for beautiful visualizations
- The open-source community for countless helpful libraries

---

**Made with ❤️ for music lovers who want to understand their listening habits better**

*Have questions? Open an issue or reach out to the maintainers!*