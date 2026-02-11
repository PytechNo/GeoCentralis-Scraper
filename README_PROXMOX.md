# Proxmox Container Setup Guide

This guide will help you set up a Proxmox LXC container to run the GeoCentralis Scraper.

## 1. Create the Container (LXC) in Proxmox

1.  **Login to Proxmox Web UI.**
2.  Click **"Create CT"** at the top right.
3.  **General Tab:**
    *   **Hostname:** `geoscraper` (or whatever you prefer)
    *   **Password:** Set a root password.
    *   **Uncheck** "Unprivileged container" if you want easier access to certain system features, but strictly a **Privileged** container is usually easier for learning/debugging, while **Unprivileged** is safer. For this guide, **Unprivileged** (default) works fine, but check "Nesting" in the Options tab later.
4.  **Template Tab:**
    *   Choose a template. We recommend **Debian 12 (Bookworm)** or **Ubuntu 22.04**.
5.  **Disks Tab:**
    *   Disk size: **8GB** or more (Chrome and cached data take space).
6.  **CPU/Memory:**
    *   **Cores:** 2 (more is better for multiple browser workers).
    *   **Memory:** 2GB (4GB recommended if running many workers).
7.  **Network:**
    *   DHCP is fine, or set a Static IP.
8.  **Confirm** and start the container.

## 2. Prepare the Container

1.  Select your new container in Proxmox.
2.  Go to **Options** -> **Features**.
3.  Edit and enable **Nesting** (usually required for some systemd features and unprivileged containers).
4.  Start the container and go to **Console**.

## 3. Install the Application

1.  **Login** as `root` with the password you set.

2.  **Transfer the project files** to the container. You can use SFTP (FileZilla) or `scp`.
    *   Target directory: `/opt/GeoCentralis-Scraper`
    *   Example using SCP from your computer:
        ```bash
        scp -r /path/to/GeoCentralis-Scraper root@<container-ip>:/opt/
        ```
    *   *Note: If SSH is not enabled by default, run `apt update && apt install openssh-server` in the container first.*

3.  **Run the Setup Script**:
    ```bash
    cd /opt/GeoCentralis-Scraper
    chmod +x setup_env.sh
    ./setup_env.sh
    ```
    *This script will update the system, install Python, Chrome, and all python requirements.*

## 4. Run the Application

First, ensure the start script is executable:

```bash
chmod +x start_app.sh
```

You can run it manually to test:

```bash
./start_app.sh
```

Or run with auto-start (imports cities and starts workers):

```bash
./start_app.sh --auto-start
```

Access the web interface at: `http://<container-ip>:8080`

## 5. Enable as a System Service (Auto-start on Boot)

To have the scraper run automatically when the container starts:

1.  Copy the service file:
    ```bash
    cp geoscraper.service /etc/systemd/system/
    ```

2.  Reload systemd and enable the service:
    ```bash
    systemctl daemon-reload
    systemctl enable geoscraper
    systemctl start geoscraper
    ```

3.  Check status:
    ```bash
    systemctl status geoscraper
    ```

## Troubleshooting

*   **Chrome fails to start:** Ensure you have enough RAM allocated to the container. Headless browsers are memory hungry.
*   **"DevToolsActivePort file doesn't exist"**: This usually means Chrome crashed immediately. Check `/opt/GeoCentralis-Scraper/geckodriver.log` or similar if generated, or run manually to see error output.
*   **Missing Dependencies:** The `setup_env.sh` tries to cover everything, but if you get `ImportError: libX...`, you might need to install additional libraries via `apt install`.
